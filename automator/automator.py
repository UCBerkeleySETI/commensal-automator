import redis
import threading
import numpy as np
import os
import subprocess
import sys
import time

from automator import redis_util
from .logger import log
from .proc_hpguppi import ProcHpguppi
from .proc_seticore import run_seticore

BFRDIR = '/home/obs/bfr5'
PROC_DOMAIN = 'blproc'
ACQ_DOMAIN = 'bluse'
DEFAULT_DWELL = 290
PROPOSAL_ID = 'BLUSE' 

class Automator(object):
    """The automator controls when the system is recording raw files, when
    the system is processing raw files, and when it's doing neither.
      
    The recording is more directly controlled by the coordinator.
    The automator instructs the coordinator when it can record by setting
    an `nshot` key in redis.

    Processing is done with slurm wrapping seticore and hpguppi_proc.

    The general sequence of events is:

    1. When a new subarray gets configured, the coordinator will notice on its
    own and allocate hosts to it. Allocating hosts doesn't necessarily mean
    the coordinator is using a lot of resources on those machines - they
    could still be intensively processing a different subarray.

    2. When the automator isn't doing any processing on a set of machines that
    the coordinator has allocated to a subarray, it uses `nshot` to
    tell the coordinator it can record on them.

    3. When the automator notices the coordinator is done recording, it runs
    processing. When processing finishes, the automator has deleted the raw files.

    4. Go back to step 2.

    In theory, this design should work for multiple subarrays simultaneously,
    although it is pretty untested at the moment so it probably doesn't work.

    Ideally, the automator would be safe to restart at any time. This also
    does not generally work yet.
    """
    def __init__(self, redis_endpoint, redis_chan, margin, 
                 hpgdomain, buffer_length, nshot_chan, nshot_msg):
        """Initialise the automator. 

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

        # A set of all hosts that are currently processing
        self.processing = set()

        # Timers maps subarray name to a timer for when we should check if it's
        # ready to process
        self.timers = {}
        
        # Setting this to True should stop all subsequent actions while we manually debug
        self.paused = False

        self.alert("starting up")


    def alert(self, message):
        redis_util.alert(self.redis_server, message, "automator")
        
        
    def start(self):
        """Start the automator."""
        self.maybe_start_recording()
        self.maybe_start_processing()
        
        ps = self.redis_server.pubsub(ignore_subscribe_messages=True)
        ps.subscribe(self.receive_channel)
        for msg in ps.listen():
            self.parse_msg(msg)

            
    def parse_msg(self, msg):
        """Handles an incoming message sent from the telescope about its state.

        msg (str): A message of format "<telescope state>:<subarray_name>"

        We handle these telescope states:

        - conf_complete
        - tracking
        - not-tracking
        - deconfigure
        """
        msg_data = msg['data'] 
        msg_components = msg_data.split(':')
        if len(msg_components) != 2:
            log.warning("Unrecognised message: {}".format(msg_data))
            return

        subarray_state, subarray_name = msg_components

        if self.paused:
            return
        log.info('subarray {} is now in state: {}'.format(subarray_name, subarray_state))
        
        self.maybe_start_recording()
        self.maybe_start_processing()
        
        if subarray_state == 'tracking':
            self.tracking(subarray_name)
        elif subarray_state == 'not-tracking':
            self.not_tracking(subarray_name)


    def get_nshot(self, subarray_name):
        """Get the current value of nshot from redis.
        """
        nshot = redis_util.get_nshot(self.redis_server, subarray_name)
        log.info("fetched nshot = {} from redis, for {}".format(nshot, subarray_name))
        return nshot
        
    
    def set_nshot(self, subarray_name, nshot):
        """Set the value of nshot in redis.
        """
        nshot_msg = self.nshot_msg.format(subarray_name, nshot)
        self.redis_server.publish(self.nshot_chan, nshot_msg)
        log.info("published nshot = {} to redis, for {}".format(nshot, subarray_name))
    
            
    def tracking(self, subarray_name):
        """Handle the telescope going into a tracking state.
           
        We don't take any action immediately, but we do want to set a timer to check
        back later to see if we are ready to process.
        """
        nshot = self.get_nshot(subarray_name)
        log.info('{} in tracking state with nshot = {}'.format(subarray_name, nshot))

        # Check if this observation has our specific proposal ID. If this is true, 
        # we want to preserve all the data in the buffers. 
        self.check_proposal_id(subarray_name)

        # If this is the last recording before the buffers will be full, 
        # we may want to process in about `DWELL` + margin seconds.
        allocated_hosts = redis_util.allocated_hosts(self.redis_server, subarray_name)
        log.info("allocated hosts for {}: {}".format(subarray_name, allocated_hosts))
        dwell = self.retrieve_dwell(allocated_hosts, DEFAULT_DWELL)
        log.info("dwell: {}".format(dwell))
        duration = dwell + self.margin

        # Start a timer to check for processing
        log.info('starting tracking timer for {} seconds'.format(duration))
        t = threading.Timer(duration, lambda: self.maybe_start_processing())
        t.start()
        self.timers[subarray_name] = t

    def check_proposal_id(self, subarray_name):
        """Check the current proposal ID. If it corresponds with 
        our own BLUSE primary time, preserve data (no processing or 
        cleanup to take place).
        """
        try:
            subarray = 'subarray_{}'.format(subarray_name[-1])
            p_id_key = '{}_observation_script_proposal_id'.format(subarray)
            p_id = self.redis_server.get('{}:{}'.format(subarray_name, p_id_key))
            if p_id == PROPOSAL_ID:
                self.alert("BLUSE proposal ID detected; pausing to preserve data.")
                self.paused = True
        except:
            self.alert("Could not retrieve proposal ID.")

    def not_tracking(self, subarray_name):
        """Handle the telescope going into a not-tracking state.

        If we had a timer waiting on the tracking event, we don't need it any more.
        """
        if subarray_name not in self.timers:
            return
        t = self.timers.pop(subarray_name)
        t.cancel()
        
        
    def pause(self, message):
        self.alert(f"{message}; pausing for debugging")
        self.paused = True


    def maybe_start_processing(self):
        """Checks if any data is ready to be processed, running processing if there is.

        This method will not return until all processing is done.
        """
        dirmap = redis_util.suggest_processing(self.redis_server,
                                               processing=self.processing)
        if not dirmap:
            return

        # Process one directory at a time to avoid race conditions
        input_dir, hosts = list(dirmap.items())[0]
        if not self.process(input_dir, hosts):
            return
        self.maybe_start_processing()


    def alert_seticore_error(self):
        """Sends a message to slack about the last seticore error."""
        host, lines = redis_util.last_seticore_error(self.redis_server)
        if host is None:
            return
        joined_lines = "\n".join(lines)
        self.alert(f"sample seticore error, from {host}:```{joined_lines}```")
        
        
    def process(self, input_dir, hosts):
        """Process raw files in the given directory and hosts. Handles the stages:

        * run seticore
        * run hpguppi_proc
        * delete the raw files

        The caller should verify that it's possible to process before calling this.

        Returns whether processing was successful.
        """
        if self.paused:
            log.info("we are paused so we do not want to process.")
            return False

        if self.processing.intersection(hosts):
            log.error("currently processing {} so cannot double-process {}".format(
                self.processing, hosts))
            return False
        self.processing = self.processing.union(hosts)

        timestamped_dir = redis_util.timestamped_dir_from_filename(input_dir)
        if timestamped_dir is None:
            self.pause("unexpected directory name: {}".format(input_dir))
            return False
        
        # Run seticore
        self.alert("running seticore...")
        result_seticore = run_seticore(sorted(hosts), BFRDIR, input_dir, timestamped_dir)
        if result_seticore > 1:
            if result_seticore > 128:
                self.pause("seticore killed with signal {}".format(result_seticore - 128))
            else:
                self.pause("the seticore slurm job failed with code {}".format(
                    result_seticore))
            self.alert_seticore_error()
            return False
        self.alert(f"seticore completed with code {result_seticore}. "
                   f"output in /scratch/data/{timestamped_dir}")
        if result_seticore > 0:
            self.alert_seticore_error()

        if False:
            # hpguppi_proc temporarily disabled
            # Run hpguppi_proc if we can
            subarray = redis_util.infer_subarray(self.redis_server, hosts)
            procstatmap = redis_util.hpguppi_procstat(self.redis_server)
            usable_hosts = set()
            for key in ["IDLE", "END"]:
                usable_hosts = usable_hosts.union(procstatmap.get(key, []))

            if subarray is None:
                self.alert("cannot run hpguppi_proc: no subarray exists for {input_dir}")
            elif not usable_hosts.issuperset(hosts):
                self.alert("cannot run hpguppi_proc: some instances are stuck")
            else:
                self.alert("running hpguppi_proc...")
                proc_hpguppi = ProcHpguppi()
                result_hpguppi = proc_hpguppi.process(PROC_DOMAIN, hosts, subarray, BFRDIR)
                if result_hpguppi != 0:
                    self.pause("hpguppi_proc timed out")
                    return False

                self.alert(f"hpguppi_proc completed. output in /scratch/data/{datadir}")

        # Clean up
        # self.alert("deleting raw files...")
        if not self.delete_buf0(hosts):
            return False
            
        self.processing = self.processing.difference(hosts)
        self.alert("processing complete.")
        self.maybe_start_recording()
        return True

        
    def delete_buf0(self, initial_hosts):
        """Deletes everything on buf0 for the provided iterable of hosts.
        Returns whether we succeeded. Pauses if we didn't.
        """
        hosts = set(initial_hosts)
        for _ in range(10):
            try:
                cmd = ['srun', 
                       '-w', 
                       ' '.join(sorted(hosts)), 
                       'bash', 
                       '-c', 
                       '/home/obs/bin/cleanmybuf0.sh --force']
                log.info(cmd)      
                result = subprocess.run(cmd)
            except Exception as e:
                self.pause("cleanmybuf0.sh failed")
                return
            still_have_raw = set(redis_util.raw_files(self.redis_server).keys())
            if result.returncode == 0 and not hosts.intersection(still_have_raw):
                # We deleted everything
                return True
            time.sleep(2)

        self.pause("failed to delete buf0 on {} hosts: {}".format(
            len(hosts), " ".join(sorted(hosts))))
        return False
    
            
    def maybe_start_recording(self):
        """Checks if any subarrays are ready to start recording, and tells the
        coordinator to start for any subarrays that are ready.

        TODO: avoid having a race condition here where we start a recording
        multiple times.
        """
        broken = redis_util.broken_daqs(self.redis_server)
        if broken:
            self.pause("{} daqs appear to be broken: {}".format(
                len(broken), " ".join(broken)))
            return
        subarrays = redis_util.suggest_recording(self.redis_server,
                                                 processing=self.processing)
        for subarray in subarrays:
            log.info("subarray {} is ready for recording".format(subarray))            
            self.set_nshot(subarray, 1)
        
        
    def retrieve_dwell(self, host_list, default_dwell):
        """Retrieve the current value of `DWELL` from the Hashpipe-Redis 
        Gateway for a specific set of hosts. 
        Note that this assumes all instances are host/0.

        Args:

            host_list (str): The list of hosts allocated to the current subarray. 

        Returns:

            DWELL (float): The duration for which the processing nodes will record
            for the current subarray (in seconds). 
        """
        dwell = default_dwell
        dwell_values = []
        for host in host_list:
            host_key = '{}://{}/0/status'.format(self.hpgdomain, host)
            host_status = self.redis_server.hgetall(host_key)
            if len(host_status) > 0:
                if 'DWELL' in host_status:
                    dwell_values.append(float(host_status['DWELL']))
                else:
                    log.warning('Cannot retrieve DWELL for {}'.format(host))
            else:
                log.warning('Cannot access {}'.format(host))
        if len(dwell_values) > 0:
            dwell = self.mode_1d(dwell_values)
            if len(np.unique(dwell_values)) > 1:
                log.warning("DWELL disagreement")    
        else:
            log.warning("Could not retrieve DWELL. Using {} sec by default.".format(default_dwell))
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

    
