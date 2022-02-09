import redis

class Automator(object):
    """The commensal automator. 
      
    The purpose of the commensal automator is to automate commensal observing,
    end-to-end. In practice, the commensal automator operates as follows:

    1. Waits for the start of a recording. When a recording starts, the list 
       of processing nodes and their Hashpipe-Redis Gateway group address are 
       retrieved. 

    2. Waits for the end of a recording. Observational parameters like `DWELL`
       (the duration for which incoming data are to be recorded for the 
       current pointing) are known, allowing the expected end-time of the 
       current recording to be calculated.  
  
 
    """
    def _init__(self, redis_endpoint, redis_channel, proc_script):
       """Initialise the automator. 

       Args: 
           redis_endpoint (str): Redis endpoint (of the form <host IP
           address>:<port>) 
           redis_channel (str): Name of the redis channel
           proc_script (str): Location of the processing script for the 
           processing script. 

       Returns:
           None
       """
       redis_host, redis_port = redis_endpoint.split(":")
       self.redis_server = redis.StrictRedis(host=redis_host, 
                                             port=redis_port, 
                                             decode_responses=True)
       self.proc_script = proc_script


