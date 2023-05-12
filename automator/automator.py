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

class Automator(object):
    """The automator controls when the system is recording raw files, when
    the system is processing raw files, and when it's doing neither.
      
    The recording is more directly controlled by the coordinator.
    The automator instructs the coordinator when it can record by setting
    a `rec_enabled:<array>` key to 1 in redis.

    Processing is done with slurm wrapping seticore and hpguppi_proc.

    The general sequence of events is:

    1. When a new subarray gets configured, the coordinator will notice on its
    own and allocate hosts to it. Allocating hosts doesn't necessarily mean
    the coordinator is using a lot of resources on those machines - they
    could still be intensively processing a different subarray.

    2. When the automator isn't doing any processing on a set of machines that
    the coordinator has allocated to a subarray, it sets the Redis key:
    `rec_enabled:<subarray name>` to 1 to tell the coordinator it can record
    on them.

    3. When the automator notices the coordinator is done recording, it runs
    processing. When processing finishes, the automator has deleted the raw files.

    4. Go back to step 2.

    In theory, this design should work for multiple subarrays simultaneously,
    although it is pretty untested at the moment so it probably doesn't work.

    Ideally, the automator would be safe to restart at any time. This also
    does not generally work yet.
    """
    def __init__(self, redis_endpoint, redis_chan, margin, 
                 hpgdomain, buffer_length, partition):
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
        """
        log.info(f"starting the automator. redis = {redis_endpoint}")
        redis_host, redis_port = redis_endpoint.split(':')
        self.redis_server = redis.StrictRedis(host=redis_host, 
                                              port=redis_port, 
                                              decode_responses=True)
        self.receive_channel = redis_chan
        self.margin = margin
        self.hpgdomain = hpgdomain
        self.buffer_length = buffer_length
        self.partition = partition

        # A set of all hosts that are currently processing
        self.processing = set()

        # Timers maps subarray name to a timer for when we should check if it's
        # ready to process
        self.timers = {}
        
        # Setting this to True should stop all subsequent actions while we manually debug
        self.paused = False

        self.alert(f"starting up, search products will be written to `/{partition}`")


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
            log.warning(f"Unrecognised message: {msg_data}")
            return

        subarray_state, subarray_name = msg_components
        log.info(f"subarray {subarray_name} is now in state: {subarray_state}")

        if self.paused:
            log.info("Paused, taking no action")
            return

        if subarray_state == 'tracking':
            self.tracking(subarray_name)
            return
        elif subarray_state == 'not-tracking':
            self.not_tracking(subarray_name) # at this point we still want to process!

        self.maybe_start_recording()
        self.maybe_start_processing()
            
    def tracking(self, subarray_name):
        """Handle the telescope going into a tracking state.
           
        We don't take any action immediately, but we do want to set a timer to check
        back later to see if we are ready to process.
        """

        log.info(f"{subarray_name} in tracking state")

        # If this is the last recording before the buffers will be full, 
        # we may want to process in about `DWELL` + margin seconds.
        allocated_hosts = redis_util.allocated_hosts(self.redis_server, subarray_name)
        log.info(f"allocated hosts for {subarray_name} are: {allocated_hosts}")
        dwell = redis_util.retrieve_dwell(self.redis_server, self.hpgdomain,
            allocated_hosts, DEFAULT_DWELL)
        log.info(f"dwell: {dwell}")
        duration = dwell + self.margin

        # Start a timer to check for processing
        log.info(f"starting tracking timer for {duration} seconds")
        t = threading.Timer(duration, lambda: self.maybe_start_processing())
        t.start()
        self.timers[subarray_name] = t


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

        # If the most recent track was a primary time track, then we don't
        # want to process yet (as there could be another primary time
        # observation coming). In this case, the coordinator keeps
        # recording enabled, which will keep the allocated instances from
        # appearing in dirmap below.

        dirmap = redis_util.suggest_processing(self.redis_server,
                                               processing=self.processing)
        if not dirmap:
            return

        # Process one directory at a time to avoid race conditions
        input_dir, hosts = list(dirmap.items())[0]
        if not self.process(input_dir, hosts, self.partition):
            return
        self.maybe_start_recording()
        self.maybe_start_processing()



    def alert_seticore_error(self):
        """Sends a message to slack about the last seticore error."""
        host, lines = redis_util.last_seticore_error(self.redis_server)
        if host is None:
            return
        joined_lines = "\n".join(lines)
        self.alert(f"sample seticore error, from {host}:```{joined_lines}```")
        
        
    def process(self, input_dir, hosts, partition):
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
            log.error(f"currently processing {self.processing} so cannot double-process {hosts}")
            return False
        self.processing = self.processing.union(hosts)

        timestamped_dir = redis_util.timestamped_dir_from_filename(input_dir)
        if timestamped_dir is None:
            self.pause(f"unexpected directory name: {input_dir}")
            return False
        
        # Run seticore
        self.alert("running seticore...")
        result_seticore = run_seticore(sorted(hosts), BFRDIR, input_dir,
                                timestamped_dir, partition, self.redis_server)
        if result_seticore > 1:
            if result_seticore > 128:
                self.pause(f"seticore killed with signal {result_seticore - 128}")
            else:
                self.pause(f"the seticore slurm job failed with code {result_seticore}")
            self.alert_seticore_error()
            return False
        self.alert(f"seticore completed with code {result_seticore}. "
                   f"output in /{partition}/data/{timestamped_dir}")
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

        # If we have just processed for a sequence of primary time
        # observations, we want to preserve the data in the buffers and
        # prevent further recording and processing.
        #NOTE: Temporary; for now we will select array_1 for primary obs:
        subarray = 'array_1'
        if redis_util.primary_sequence_end(self.redis_server, subarray):
            log.info("Primary sequence processed, therefore pausing.")
            self.paused = True
        else:
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

        self.pause(f"failed to delete buf0 on {len(hosts)} hosts: " + " ".join(sorted(hosts)))
        return False
    
            
    def maybe_start_recording(self):
        """Checks if any subarrays are ready to start recording, and tells the
        coordinator to start for any subarrays that are ready.

        TODO: avoid having a race condition here where we start a recording
        multiple times.
        """
        broken = redis_util.broken_daqs(self.redis_server)
        if broken:
            self.pause(f"{len(broken)} daqs appear to be broken: " + " ".join(broken))
            return
        subarrays = redis_util.suggest_recording(self.redis_server,
                                                 processing=self.processing)
        for subarray in subarrays:
            # If a primary sequence has ended, we don't want to record
            # the next track.
            if redis_util.primary_sequence_end(self.redis_server, subarray):
                log.info(f"A primary sequence has ended for {subarray},"
                " therefore not recording.")
            else:
                log.info(f"subarray {subarray} is ready for recording")
                redis_util.enable_recording(self.redis_server, subarray)