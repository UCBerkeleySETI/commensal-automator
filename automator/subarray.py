class Subarray(object):
      """Tracks the automator's internal state for a single subarray.

      In general, whenever we store information both in the Subarray object
      and in redis, it makes it possible to have the sort of bug where
      the data here is out of sync with redis. So it's better to avoid putting
      data here when it is canonically stored in redis.
      """
      def __init__(self, name, state):
          """Initialise the subarray with known parameters. 
          
          Args:
              
              name (str): The name of the current subarray. 
              state (str): The observational stage of the current subarray. 

          Returns:

              None
          """
          self.name = name
          self.state = state
          self.processing = False


