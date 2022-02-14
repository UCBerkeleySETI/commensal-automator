from .logger import log

class Subarray(obj):
      """Contains all the attributes of a particular subarray. It is also used
      to keep track of the state of the subarray as it changes during an 
      observation.  
      """
      def __init__(self, name, state, nshot, dwell, start_ts, margin, hosts):
          """Initialise the subarray with known parameters. 
          
          Args:
              
              name (str): The name of the current subarray. 
              state (str): The observational stage of the current subarray. 
              nshot (int): The number of recordings still to be made for the 
              current subarray. 
              dwell (float): The duration of each recording for the current 
              subarray.
              start_ts (float): The timestamp at which a recording has started
              or at which processing has started (UNIX time). 
              margin (float): The safety margin in seconds to add to DWELL 
              when determining the duration of a recording. 
              hosts (list): A list of the host names (str) allocated to record
              and process data from the current subarray. 

          Returns:

              None
          """
          self.name = name
          self.state = state
          self.nshot = nshot
          self.dwell = dwell
          self.start_ts = start_ts
          self.margin = margin
          self.allocated_hosts = hosts
          log.info("New subarray object for {}".format(name))

