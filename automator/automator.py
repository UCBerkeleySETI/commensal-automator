import redis
import datetime

from .logger import log, set_logger
from .subarray import Subarray

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
    def _init__(self, redis_endpoint, redis_channel, proc_script, margin):
        """Initialise the automator. 

        Args: 

            redis_endpoint (str): Redis endpoint (of the form <host IP
            address>:<port>) 
            redis_channel (str): Name of the redis channel
            proc_script (str): Location of the processing script for the 
            processing script. 
            margin (float): Safety margin (in seconds) to add to `DWELL`
            when calculating the estimated end of a recording. 

        Returns:

            None
        """
        set_logger('DEBUG')
        log.info('Starting Automator:\n'
                 'Redis endpoint: {}\n'
                 'Processing script: {}\n'.format(redis_endpoint, proc_script))
        redis_host, redis_port = redis_endpoint.split(':')
        self.redis_server = redis.StrictRedis(host=redis_host, 
                                              port=redis_port, 
                                              decode_responses=True)
        self.receive_channel = redis_channel
        self.proc_script = proc_script
        self.margin = margin
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
        msg_components = msg.split(':')
        if((len(msg_components) !=  2):
            log.warning("Unrecognised message: {}".format(msg))
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
           self.red.llen(array_key))        
       # DWELL, the duration of a recording in seconds
       dwell = self.retrieve_dwell(self, allocated_hosts)
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





 
