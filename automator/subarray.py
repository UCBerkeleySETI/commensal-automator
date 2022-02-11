from .logger import log

class Subarray(obj):
      """Contains all the attributes of a particular subarray. It is also used
      to keep track of the state of the subarray as it changes during an 
      observation.  
      """
      def __init__(self, name, stage, nshot, dwell, start_ts, margin):
          """Initialise the subarray with known parameters. 
          
          Args:
              
              name (str): The name of the current subarray. 
              stage (str): The observational stage of the current subarray. 
              nshot (int): The number of recordings still to be made for the 
              current subarray. 
              dwell (float): The duration of each recording for the current 
              subarray.
              start_ts (float): The timestamp at which a recording has started
              (UNIX time). 
              margin (float): The safety margin in seconds to add to DWELL 
              when determining the duration of a recording. 

          Returns:

              None
          """
          self.name = name
          self.stage = stage
          self.nshot = nshot
          self.dwell = dwell
          self.start_ts = start_ts
          self.margin = margin
          log.info("New subarray object for {}".format(name))
