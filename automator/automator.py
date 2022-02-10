import redis

from .logger import log, set_logger

class Automator(object):
    """The commensal automator. 
      
    The purpose of the commensal automator is to automate commensal observing,
    end-to-end. In practice, the commensal automator operates as follows:

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
  
    3. Processing. Processing does not take place if `nshot > 0`. In general, 
       this means that processing will only take place once the buffers are 
       full.

       A script is called which runs all processing across the processing
       nodes assigned to the current subarray. Multiple processing steps
       may take place simultaneously and/or sequentially. 
      
       The location of the script is retrieved from Redis. 

    4. Clearing the buffers.      
 
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
       redis_host, redis_port = redis_endpoint.split(":")
       self.redis_server = redis.StrictRedis(host=redis_host, 
                                             port=redis_port, 
                                             decode_responses=True)
       self.proc_script = proc_script
       self.margin = margin
       set_logger("DEBUG")
