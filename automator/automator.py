import redis
from datetime import datetime
import threading
import numpy
import subprocess

from .logger import log
from .subarray import Subarray
from .proc_hpguppi import ProcHpguppi
from .proc_seticore import ProcSeticore

#Temporary hard coding:
MAX_DRIFT = 10.0
SNR = 10.0
TEL_ID = 64
NUM_BANDS = 16
FFT_SIZE = 131072
BFRDIR = '/home/obs/bfr5'
OUTPUTDIR = '/scratch/test.20220728' 
INPUTDIR = '/buf0ro/20220720/0009/Unknown/GUPPI'
PROC_DOMAIN = 'blproc'

class Automator(object):
    """The commensal automator. 
      
    The purpose of the commensal automator is to automate commensal observing,
    end-to-end. The automator must be started before a recording has started. 
    If it is not, then the current recording will be skipped. 

    In practice, the commensal automator operates as follows:

    1. Waits for the start of a recording. When a recording starts, the list 
       of processing nodes and their Hashpipe-Redis Gateway group address are 
       retrieved. 

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
    def __init__(self, redis_endpoint, redis_chan, proc_script, margin, 
                 hpgdomain, buffer_length, nshot_chan, nshot_msg):
        """Initialise the automator. 

        Args: 

            redis_endpoint (str): Redis endpoint (of the form <host IP
            address>:<port>) 
            redis_chan (str): Name of the redis channel
            proc_script (str): Location of the processing script.
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
        log.info('Starting Automator:\n'
                 'Redis endpoint: {}\n'
                 'Processing script: {}\n'.format(redis_endpoint, proc_script))
        redis_host, redis_port = redis_endpoint.split(':')
        self.redis_server = redis.StrictRedis(host=redis_host, 
                                              port=redis_port, 
                                              decode_responses=True)
        self.receive_channel = redis_chan
        self.margin = margin
        self.hpgdomain = hpgdomain
        self.buffer_length = buffer_length
        self.proc_script = proc_script
        # Future work: split off Redis info into its own module
        self.nshot_chan = nshot_chan
        self.nshot_msg = nshot_msg
        self.active_subarrays = {}

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
 
            - configure
            - tracking
            - not-tracking
            - deconfigure
            - processing
            - processing-complete

            If the <message> component of msg is one of the above, then the 
            first component of msg is the subarray name. 

        Returns:

            None
        """
        msg_data = msg['data'] 
        msg_components = msg_data.split(':')
        if(len(msg_components) !=  2):
            log.warning("Unrecognised message: {}".format(msg_data))
        else:
            subarray_state = msg_components[0]
            subarray_name = msg_components[1]
            self.change_state(subarray_state)(subarray_name)
 
    def change_state(self, state):
        """Select and return the function corresponding to the change in state
        of the subarray. A dictionary is used since Python's `match-case` switch
        statement implementation is only available in 3.10.

        For all unrecognised states, the same default `ignored_state` function
        is returned. 

        Args:

            state (str): New subarray state (one of configure, tracking, 
            not-tracking, deconfigure, processing, processing-complete).

        Returns:

            None
        """
        log.info('New state: {}'.format(state))
        states = {'configure':self.configure, 
                  'tracking':self.tracking, 
                  'not-tracking':self.not_tracking, 
                  'deconfigure':self.deconfigure, 
                  'processing':self.processing, 
                  'processing-complete':self.processing_complete}
        return states.get(state, self.ignored_state)

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
        nshot = self.redis_server.get(nshot_key).split(':')[1] 
        # `allocated_hosts` is the list of host names assigned to record and
        # process incoming data from the current subarray. 
        allocated_hosts_key = 'coordinator:allocated_hosts:{}'.format(subarray_name)
        allocated_hosts = self.redis_server.lrange(allocated_hosts_key, 0,
            self.redis_server.llen(allocated_hosts_key))        
        # DWELL, the duration of a recording in seconds
        dwell = self.retrieve_dwell(allocated_hosts)
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

    def ignored_state(self, subarray_name):
        """This function is to be executed if an unrecognised state (or a 
        state not relevant to the `automator`) is received.

        Args:

            subarray_name (str): The name of the subarray for which an 
            unrecognised or ignored state has been received. 

        Returns:
           
            None
        """
        log.info('Ignoring irrelevant state for {}'.format(subarray_name))

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
        self.subarray_init(subarray_name, 'configure')

    def tracking(self, subarray_name):
        """These actions are taken when an existing subarray has begun to 
        track a source.  

        Args:
           
            subarray_name (str): The name of the subarray which has just begun
            to track a new source. 

        Returns:
      
            None
        """
        if(subarray_name not in self.active_subarrays):
            self.init_subarray(subarray_name, 'tracking')
        else:
            # Update `nshot` (the number of recordings still to be taken)
            nshot_key = 'coordinator:trigger_mode:{}'.format(subarray_name)
            # `nshot` is retrieved in the format: `nshot:<n>`
            nshot = self.redis_server.get(nshot_key).split(':')[1] 
            self.active_subarrays[subarray_name].nshot = nshot  
        if(self.active_subarrays[subarray_name].nshot == 0):
            start_ts = datetime.utcnow()
            self.active_subarrays[subarray_name].start_ts = start_ts
            # If this is the last recording before the buffers will be full, 
            # start a timer for `DWELL` + margin seconds. 
            duration = self.active_subarrays[subarray_name].dwell + self.margin
            # The state to transition to after tracking is processing. 
            self.active_subarrays[subarray_name].tracking_timer = threading.Timer(duration, 
                lambda:self.timeout('tracking', 'processing', subarray_name))
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
        log.info('{} deconfigured. Proceeding'
                 ' to processing.'.format(subarray_name))
        log.info('Ordinarily, `nshot` would now be set to 0.')
        log.info('Leaving `nshot` unaffected for diagnostic purposes.')
        #nshot_msg = self.nshot_msg.format(subarray_name, 0)
        #self.redis_server.publish(self.nshot_chan, nshot_msg) 
        self.change_state('processing')(subarray_name)

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
        if(hasattr(self.active_subarrays[subarray_name], 'tracking_timer')):
            self.active_subarrays[subarray_name].tracking_timer.cancel()
            del self.active_subarrays[subarray_name].timer
            if(self.active_subarrays[subarray_name].nshot == 0):
                log.info('Final recording completed. Moving to processing state.')
                self.change_state('processing')(subarray_name)
        else:
           log.info('No active recording for end'
                    'of track for {}'.format(subarray_name)) 

    def processing(self, subarray_name):
        """Initiate processing across processing nodes in the current subarray. 

        Args:
           
            subarray_name (str): The name of the subarray for which data is to 
            be processed. 

        Returns:
      
            None
        """
        # Future work: add a timeout for the script.
        # Retrieve the list of hosts assigned to the current subarray:
        host_key = 'coordinator:allocated_hosts:{}'.format(subarray_name)
        instance_list = self.redis_server.lrange(host_key, 
                                             0, 
                                             self.redis_server.llen(host_key))
        # Format for host name (rather than instance name):
         
        host_list =  [host.split('/')[0] for host in instance_list]
        #processing = ProcHpguppi()
        #processing.process(PROC_DOMAIN, host_list, subarray_name, BFRDIR, OUTPUTDIR)
        proc = ProcSeticore()
        proc.process('/home/lacker/bin/seticore-0.1.7', host_list, BFRDIR, subarray_name)

        # Release hosts:
        # Get list of currently available hosts:
        if(self.redis_server.exists('coordinator:free_hosts')):
            free_hosts = self.redis_server.lrange('coordinator:free_hosts', 0,
                self.redis_server.llen('coordinator:free_hosts'))
            self.redis_server.delete('coordinator:free_hosts')
        else:
            free_hosts = []
        # Append released hosts and write 
        free_hosts = free_hosts + instance_list
        log.info(free_hosts)
        self.redis_server.rpush('coordinator:free_hosts', *free_hosts)    
        # Remove resources from current subarray 
        self.redis_server.delete('coordinator:allocated_hosts:{}'.format(subarray_name))
        log.info("Released {} hosts; {} hosts available".format(len(instance_list),
                len(free_hosts)))

        #slurm_cmd = ['sbatch', '-w', host_list, self.proc_script]
        
        #log.info('Running processing (slurm seticore) for hosts: {}'.format(host_list))
        #try:
#            subprocess.Popen(slurm_cmd)
        #     self.proc_slurm(host_list, BFRDIR, OUTPUTDIR, INPUTDIR)
        #except:
        #    log.error('Could not run script for {}'.format(subarray_name))

    def slurm_cmd(self, host_list, proc_str):
        """Run slurm processing script.

        Args:
            proc_script (str): Path to processing shell script. 
 
        Returns:
            'success' if slurm processing script finished executing. 
            'failed' if the provided script could not be run. 
        """
        slurm_cmd = ['srun', '-w'] + host_list + proc_str
        log.info('Running processing command: {}'.format(slurm_cmd))
        try:
            subprocess.run(slurm_cmd)
            return 'success'
        except Exception as e:
            log.error('Could not run command')
            log.error(e)
            return 'failed'

    def proc_slurm(self, hosts, bfrdir, outputdir, inputdir):
        """Processing for minimal BLUSE SETI survey.
        For use with processing stages that do not use the Hashpipe-Redis Gateway.
        """

        # Parsing input:
        if(hosts is None):
            log.error('Please provide a list of hosts, or \'all\'')
            sys.exit()
        elif((len(hosts) == 1) & (hosts[0] == 'all')):
            hosts = []
            for i in range(0, 64):
                hosts.append('blpn{}'.format(i))

        proc_str = ['/home/lacker/seticore/build/seticore',
                    '--input={}'.format(inputdir),
                    '--output={}'.format(outputdir),
                    '--max_drift={}'.format(MAX_DRIFT),
                    '--snr={}'.format(SNR),
                    '--recipe_dir={}'.format(bfrdir),
                    '--num_bands={}'.format(NUM_BANDS),
                    '--fft_size={}'.format(FFT_SIZE),
                    '--telescope_id={}'.format(TEL_ID)]

        # Run slurm command
        outcome = self.slurm_cmd(hosts, proc_str)

        # Final cleanup:
        log.info('Any other final steps go here')



    def processing_complete(self, subarray_name):
        """Actions to be taken once processing is complete for the  current 
        subarray. 

        It is assumed that as part of the processing, the NVMe buffers have 
        been emptied. 

        Args:
           
            subarray_name (str): The name of the subarray for which processing
            has completed. 

        Returns:
      
            None
        """
        dwell = self.active_subarrays[subarray_name].dwell
        new_nshot = np.floor(self.buffer_length/dwell)
        # Reset nshot by publishing to the appropriate channel 
        nshot_msg = self.nshot_msg.format(subarray_name, new_nshot)
        self.redis_server.publish(self.nshot_chan, nshot_msg)        

    def timeout(self, next_state, subarray_name):
        """When a timeout happens (for completion of recording or processing,
        for example), this function initiates the appropriate state transition
        (if it should take place). 

        For example, the telescope may have stopped recording data before the
        completion of `DWELL`, in which case the state transition will have 
        already taken place and this timeout function will never be called.  

        If the transition to the next state is to take place, then the
        relevant function is called. 

        Args:         

            subarray_name (str): The name of the subarray whose state is 
            subject to change
            next_state (str): The state into which the subarray would 
            transition into after current_state. 

        Returns:

           None
        """
        # An additional check may be performed by reading the status of `NETSTAT`
        # in the Hashpipe-Redis Gateway status buffer. 
        self.change_state(next_state)(subarray_name)

    def retrieve_dwell(self, host_list):
        """Retrieve the current value of `DWELL` from the Hashpipe-Redis 
        Gateway for a specific set of hosts. 

        Args:

            host_list (str): The list of hosts allocated to the current subarray. 

        Returns:

            DWELL (float): The duration for which the processing nodes will record
            for the current subarray (in seconds). 
        """
        dwell = 0
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
            log.warning("Could not retrieve DWELL")

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
