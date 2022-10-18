import redis
from datetime import datetime, timezone
import threading
import numpy as np
import subprocess
import sys

from .logger import log
from .subarray import Subarray
from .proc_hpguppi import ProcHpguppi
from .proc_seticore import ProcSeticore

BFRDIR = '/home/obs/bfr5'
PROC_DOMAIN = 'blproc'
ACQ_DOMAIN = 'bluse'
SLACK_CHANNEL = "meerkat-obs-log"
SLACK_PROXY_CHANNEL = "slack-messages"

class Automator(object):
    """The commensal automator. 
      
    In practice, the commensal automator operates as follows:

    1. The first time it gets an event for a subarray, the list of processing
       nodes and their Hashpipe-Redis Gateway group address are retrieved. 

    2. Waits for the end of a recording. Observational parameters like `DWELL`
       (the duration for which incoming data are to be recorded for the 
       current pointing) are retrieved, allowing the expected end-time of the 
       current recording to be calculated.  

       It is possible for a recording to end, or for the current subarray to 
       be deconfigured, before `DWELL` seconds have passed. The automator is 
       notified of these eventualities via Redis channels. 

       The automator also retrieves the current state of `nshot` (the number 
       of recordings of length `DWELL` to take prior to processing). 
       Currently, `nshot` is decremented independently by another process (in  
       the case of MeerKAT, the `coordinator`) each time a recording has been 
       made successfully.

       `nshot` can also be set manually - see the `coordinator` documentation
       for further information on how this is implemented at MeerKAT. 

       If one of the following is met, and `nshot > 0`:
       
           - `DWELL` and an additional safety margin of time has passed. 
           - The recording ends before `DWELL` + the safety margin has passed.

       Then, the automator waits for the next recording (as in 1 above). If
       one of the above takes place and `nshot` is 0, then processing (in 3 
       below) takes place. 

       If the current subarray is deconfigured before `DWELL` and the safety 
       margin have passed, `nshot` is set to 0 and processing (in 3 below) 
       is commenced. 
  
    3. Initiates processing tasks. Processing does not take place if 
       `nshot > 0`. In general, this means that processing will only
       take place once the buffers are full.

       A script is called which runs all processing across the processing
       nodes assigned to the current subarray. Multiple processing steps
       may take place simultaneously and/or sequentially. 
      
       The location of the script is retrieved from Redis. 

    4. Waits for all processing tasks to finish (once the script has finished
       running or a timeout duration is reached). 
    
    5. Clearing the buffers. Essentially, this is just another processing
       task (which only takes place at the end of all other processing 
       tasks). Circus or slurm or ssh could be used to accomplish this across
       processing nodes.

    6. Resets `nshot` to re-enable recording (for the current processing nodes
       allocated to the current subarray). 

    7. Returns to the waiting state (see 1).     
 
    """
    def __init__(self, redis_endpoint, redis_chan, margin, 
                 hpgdomain, buffer_length, nshot_chan, nshot_msg):
        """Initialise the automator. 

        Args: 

            redis_endpoint (str): Redis endpoint (of the form <host IP
            address>:<port>) 
            redis_chan (str): Name of the redis channel
            margin (float): Safety margin (in seconds) to add to `DWELL`
            when calculating the estimated end of a recording. 
            hpgdomain (str): The Hashpipe-Redis Gateway domain for the instrument
            in question. 
            buffer_length (float): Maximum duration of recording (in seconds)
            for the maximum possible incoming data rate. 
            nshot_chan (str): The Redis channel for resetting nshot.
            nshot_msg (str): The base form of the Redis message for resetting
            nshot. For example, `coordinator:trigger_mode:<subarray_name>:nshot:<n>`

        Returns:

            None
        """
        log.info('starting the automator. redis = {}'.format(redis_endpoint))
        redis_host, redis_port = redis_endpoint.split(':')
        self.redis_server = redis.StrictRedis(host=redis_host, 
                                              port=redis_port, 
                                              decode_responses=True)
        self.receive_channel = redis_chan
        self.margin = margin
        self.hpgdomain = hpgdomain
        self.buffer_length = buffer_length
        self.nshot_chan = nshot_chan
        self.nshot_msg = nshot_msg
        self.active_subarrays = {}
        self.alert("starting the automator at " +
                   datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z"))
        
    def start(self):
        """Start the automator. Actions to be taken depend on the incoming 
        observational stage messages on the appropriate Redis channel.  
    
        Args:

            None
 
        Returns:

            None
        """
        ps = self.redis_server.pubsub(ignore_subscribe_messages=True)
        ps.subscribe(self.receive_channel)
        for msg in ps.listen():
            self.parse_msg(msg)

    def parse_msg(self, msg):
        """Examines an incoming message (from the appropriate Redis channel)
        and determines whether or not it is relevant to the automator. If it is
        relevent, the appropriate state-change function is called. 

        Args:

            msg (str): A message to be processed (as would arrive from the 
            appropriate Redis pub/sub channel. Desired messages are of the 
            format <message>:<subarray_name>.

            Messages that are responded to are as follows:
 
            - conf_complete
            - tracking
            - not-tracking
            - deconfigure

            If the <message> component of msg is one of the above, then the 
            first component of msg is the subarray name. 

        Returns:

            None
        """
        msg_data = msg['data'] 
        msg_components = msg_data.split(':')
        if(len(msg_components) !=  2):
            log.warning("Unrecognised message: {}".format(msg_data))
            return

        subarray_state = msg_components[0]
        subarray_name = msg_components[1]

        log.info('subarray {} is now in state: {}'.format(subarray_name, subarray_state))

        if subarray_state == 'conf_complete':
            self.configure(subarray_name)
        elif subarray_state == 'tracking':
            self.tracking(subarray_name)
        elif subarray_state == 'not-tracking':
            self.not_tracking(subarray_name)
        elif subarray_state == 'deconfigure':
            self.deconfigure(subarray_name)
        else:
            log.info('no handler for state {}, ignoring message'.format(subarray_state))

            
    def subarray_init(self, subarray_name, subarray_state):
        """Initialise a subarray. This means retrieving appropriate metadata 
        for the current subarray. This step must take place before a recording
        has started. 

        Args:

            subarray_name (str): The name of the new subarray whose state is
            to be tracked.
            subarray_state (str): The current state of the new subarray. 

        Returns:

            None
        """
        # `nshot`, the number of recordings still to be taken
        nshot_key = 'coordinator:trigger_mode:{}'.format(subarray_name)
        # `nshot` is retrieved in the format: `nshot:<n>`
        nshot = int(self.redis_server.get(nshot_key).split(':')[1])
        # `allocated_hosts` is the list of host names assigned to record and
        # process incoming data from the current subarray. 
        allocated_hosts_key = 'coordinator:allocated_hosts:{}'.format(subarray_name)
        allocated_hosts = self.redis_server.lrange(allocated_hosts_key, 0,
            self.redis_server.llen(allocated_hosts_key))        
        # DWELL, the duration of a recording in seconds
        # Set default to 300 seconds here (this will be updated prior
        # to recording).
        dwell = 300
        # If the subarray state is `tracking` or `processing`, we can simply
        # assume that this transition has just happened and record `start_ts`
        # as the current time. This is because if the start of a recording
        # has been missed, it will be ignored by the `automator`.  
        start_ts = datetime.utcnow()
        subarray_obj = Subarray(subarray_name, 
                                subarray_state, 
                                nshot, 
                                dwell, 
                                start_ts, 
                                self.margin, 
                                allocated_hosts)
        self.active_subarrays[subarray_name] = subarray_obj
        log.info('Initialised new subarray object: {}'
                 ' in state {}'.format(subarray_name, subarray_state))


    def configure(self, subarray_name):
        """This function is to be run on configuration of a new subarray. 

        Args:
           
            subarray_name (str): The name of the subarray which has just been
            configured. 

        Returns:
      
            None
        """
        # Since a subarray only exists after configuration, this is the first 
        # possible time a subarray object can have been created. 
        # Subarray to be initialised after `conf_complete` message.
        self.subarray_init(subarray_name, 'conf_complete')

    def tracking(self, subarray_name):
        """These actions are taken when an existing subarray has begun to 
        track a source.  

        Args:
           
            subarray_name (str): The name of the subarray which has just begun
            to track a new source. 

        Returns:
      
            None
        """
        if subarray_name not in self.active_subarrays:
            self.subarray_init(subarray_name, 'tracking')

        if self.active_subarrays[subarray_name].processing:
            log.info('The backend is still processing, so no action taken.')
            return
            
        # Update `nshot` (the number of recordings still to be taken)
        nshot_key = 'coordinator:trigger_mode:{}'.format(subarray_name)
        # `nshot` is retrieved in the format: `nshot:<n>`
        nshot = int(self.redis_server.get(nshot_key).split(':')[1])
        self.active_subarrays[subarray_name].nshot = nshot  
        self.active_subarrays[subarray_name].state = 'tracking' 
        log.info('{} in tracking state with nshot = {}'.format(subarray_name, nshot))
        #if(self.active_subarrays[subarray_name].nshot == '1'):
        log.info('Preparing for processing.')
        start_ts = datetime.utcnow()
        self.active_subarrays[subarray_name].start_ts = start_ts
        # If this is the last recording before the buffers will be full, 
        # start a timer for `DWELL` + margin seconds.
        allocated_hosts = self.active_subarrays[subarray_name].allocated_hosts
        log.info(allocated_hosts) 
        dwell = self.retrieve_dwell(allocated_hosts)
        log.info(dwell)
        self.active_subarrays[subarray_name].dwell = dwell
        duration = dwell + self.margin
        # The state to transition to after tracking is processing. 
        log.info('Starting tracking timer for {} seconds'.format(duration))
        self.active_subarrays[subarray_name].tracking_timer = threading.Timer(duration, 
            lambda: self.processing(subarray_name))
        self.active_subarrays[subarray_name].tracking_timer.start()
        

    def deconfigure(self, subarray_name):
        """If a deconfigure message is received (indicating that the current
        subarray has been deconfigured), `nshot` is set to 0 and processing is 
        continued. 

        Args:
      
            subarray_name (str): Name of the current subarray that has just been 
            deconfigured. 

        Returns:
 
            None
        """
        if subarray_name not in self.active_subarrays:
            self.subarray_init(subarray_name, 'deconfigure')
        
        self.active_subarrays[subarray_name].state = 'deconfigure'  
        if(self.active_subarrays[subarray_name].processing):
            log.info('Processing in progress...')
        else:
            log.info('{} deconfigured. Proceeding'
                 ' to processing.'.format(subarray_name))
            nshot_msg = self.nshot_msg.format(subarray_name, 0)
            self.redis_server.publish(self.nshot_chan, nshot_msg) 
            self.processing(subarray_name)

    def not_tracking(self, subarray_name):
        """These actions are taken when an existing subarray has ceased to 
        track a source.  


        If a timer has previously been set (and therefore tracking of the 
        current source has ended prematurely), cancel the timer and (if 
        `nshot > 0`), wait for the next source to be tracked. If 
        `nshot == 0`, then transition to the processing state.  

        Args:
           
            subarray_name (str): The name of the subarray which has just
            ceased to track a specific source. 

        Returns:
      
            None
        """
        if subarray_name not in self.active_subarrays:
            self.subarray_init(subarray_name, 'not-tracking')
        subarray = self.active_subarrays[subarray_name]
            
        if hasattr(subarray, 'tracking_timer'):
            subarray.tracking_timer.cancel()
            del subarray.tracking_timer
            if subarray.nshot == 0:
                log.info(subarray_name + ' has nshot = 0, so it is ready to process.')
                self.processing(subarray_name)
            else:
                log.info('{} has nshot = {}, so it is not ready to process.'.format(
                    subarray_name, subarray.nshot))
        else:
           log.info('We are not recording for {}, so there is nothing to process.'.format(
               subarray_name))

           
    def processing(self, subarray_name):
        """Initiate processing across processing nodes in the current subarray. 

        Args:
           
            subarray_name (str): The name of the subarray for which data is to 
            be processed. 

        Returns:
      
            None
        """
        subarray = self.active_subarrays[subarray_name]
        log.info(subarray_name + " is now processing.")
        if subarray.processing:
            log.error("we are already processing {} - we cannot double-process it.".format(
                subarray_name))
            return
        subarray.processing = True 
        # Future work: add a timeout for the script.
        # Retrieve the list of hosts assigned to the current subarray:
        host_key = 'coordinator:allocated_hosts:{}'.format(subarray_name)
        instance_list = self.redis_server.lrange(host_key, 
                                             0, 
                                             self.redis_server.llen(host_key))
        # Format for host name (rather than instance name):
        host_list =  [host.split('/')[0] for host in instance_list]
        # DATADIR
        datadir = self.redis_server.get('{}:current_sb_id'.format(subarray_name))
        # seticore processing
        alert_msg = "Initiating processing with seticore..."
        self.alert(alert_msg)
        proc = ProcSeticore()
        result_seticore = proc.process('/home/lacker/bin/seticore', host_list, BFRDIR, subarray_name)        
        if(result_seticore > 1):
            alert_msg = "Seticore returned code {}. Stopping automator for debugging.".format(result_seticore)
            log.error(alert_msg)
            self.alert(alert_msg)
            alert_logs = "Log files available by node: `/home/obs/seticore_slurm/`"
            log.error(alert_logs)
            self.alert(alert_logs)
            sys.exit(0)
        else:
            alert_msg = "New recording processed by seticore (code {}).".format(result_seticore) 
            log.info(alert_msg)
            self.alert(alert_msg)
            alert_output = "Output data are available in /scratch/data/{}".format(datadir)
            log.info(alert_output)
            self.alert(alert_output)
        # hpguppi_processing
        alert_msg = "Initiating processing with hpguppi_proc..." 
        self.alert(alert_msg)
        proc_hpguppi = ProcHpguppi()
        result_hpguppi = proc_hpguppi.process(PROC_DOMAIN, host_list, subarray_name, BFRDIR)
        if(result_hpguppi != 0):
            alert_msg = "hpguppi_proc timed out. Stopping automator for debugging."
            log.error(alert_msg)
            self.alert(alert_msg)
            sys.exit(0)
        else:
            alert_msg = "New recording processed by hpguppi_proc. Output data are available in /scratch/data/{}".format(datadir)
            log.info(alert_msg)
            self.alert(alert_msg)
        self.cleanup(subarray_name)

    def cleanup(self, subarray_name):
        """The last part of processing, removing files and getting ready for future recordings.

        Args:
           
            subarray_name (str): The name of the subarray we are cleaning up.

        Returns:
      
            None
        """
        host_key = 'coordinator:allocated_hosts:{}'.format(subarray_name)
        instance_list = self.redis_server.lrange(host_key, 
                                             0, 
                                             self.redis_server.llen(host_key))

        # Empty the NVMe modules
        # Format for host name (rather than instance name):
        hosts =  [host.split('/')[0] for host in instance_list]
        try:
            cmd = ['srun', 
                   '-w', 
                   ' '.join(hosts), 
                   'bash', 
                   '-c', 
                   '/home/obs/bin/cleanmybuf0.sh --force']
            log.info(cmd)      
            subprocess.run(cmd)
        except Exception as e:
            log.error('Could not empty all NVMe modules')
            print(e)

        # Release hosts if current subarray has already been deconfigured:
        if(self.active_subarrays[subarray_name].state == 'deconfigure'): 
            proc_group = '{}:{}///gateway'.format(PROC_DOMAIN, subarray_name)
            self.redis_server.publish(proc_group, 'leave={}'.format(subarray_name))
            acq_group = '{}:{}///gateway'.format(ACQ_DOMAIN, subarray_name)
            self.redis_server.publish(acq_group, 'leave={}'.format(subarray_name))
            # Get list of currently available hosts:
            if(self.redis_server.exists('coordinator:free_hosts')):
                free_hosts = self.redis_server.lrange('coordinator:free_hosts', 0,
                    self.redis_server.llen('coordinator:free_hosts'))
                self.redis_server.delete('coordinator:free_hosts')
            else:
                free_hosts = []
            # Append released hosts and write 
            free_hosts = free_hosts + instance_list
            self.redis_server.rpush('coordinator:free_hosts', *free_hosts)    
            # Remove resources from current subarray 
            self.redis_server.delete('coordinator:allocated_hosts:{}'.format(subarray_name))
            log.info("Released {} hosts; {} hosts available".format(len(instance_list),
                    len(free_hosts)))

        #dwell = self.active_subarrays[subarray_name].dwell
        #new_nshot = np.floor(self.buffer_length/dwell)
        new_nshot = 1
        # Reset nshot by publishing to the appropriate channel 
        nshot_msg = self.nshot_msg.format(subarray_name, new_nshot)
        log.info('Resetting nshot after processing: {} {}'.format(self.nshot_chan, nshot_msg))
        self.redis_server.publish(self.nshot_chan, nshot_msg)        
        self.active_subarrays[subarray_name].processing = False 
        log.info(subarray_name + ' is done processing.')

        
    def retrieve_dwell(self, host_list):
        """Retrieve the current value of `DWELL` from the Hashpipe-Redis 
        Gateway for a specific set of hosts. Defaults to 300 seconds. 

        Args:

            host_list (str): The list of hosts allocated to the current subarray. 

        Returns:

            DWELL (float): The duration for which the processing nodes will record
            for the current subarray (in seconds). 
        """
        dwell = 300
        dwell_values = []
        for host in host_list:
            host_key = '{}://{}/status'.format(self.hpgdomain, host)
            host_status = self.redis_server.hgetall(host_key)
            if(len(host_status) > 0):
                if('DWELL' in host_status):
                    dwell_values.append(float(host_status['DWELL']))
                else:
                    log.warning('Cannot retrieve DWELL for {}'.format(host))
            else:
                log.warning('Cannot access {}'.format(host))
        if(len(dwell_values) > 0):
            dwell = self.mode_1d(dwell_values)
            if(len(np.unique(dwell_values)) > 1):
                log.warning("DWELL disagreement")    
        else:
            log.warning("Could not retrieve DWELL. Using 300 sec by default.")
        return dwell

    def mode_1d(self, data_1d):
        """Calculate the mode of a one-dimensional list. 

        Args:

            data_1d (list): List of values for which to calculate the mode. 

        Returns:

            mode_1d (float): The most common value in the list.
        """
        vals, freqs = np.unique(data_1d, return_counts=True)
        mode_index = np.argmax(freqs)
        mode_1d = vals[mode_index]
        return mode_1d

    def alert(self, message, slack_channel=SLACK_CHANNEL,
              slack_proxy_channel=SLACK_PROXY_CHANNEL):
        """Publish a message to the alerts Slack channel. 

        Args:
            message (str): Message to publish to Slack.  
            slack_channel (str): Slack channel to publish message to. 
            slack_proxy_channel (str): Redis channel for the Slack proxy/bridge. 

        Returns:
            None  
        """
        # Format: <Slack channel>:<Slack message text>
        alert_msg = '{}:{}'.format(slack_channel, message)
        self.redis_server.publish(slack_proxy_channel, alert_msg)
