from automator.logger import log
from automator import redis_util
from automator import recproc_utils as recproc

DEFAULT_DWELL = 290 # in seconds

class State(object):
    """State object for use with the coordinator state machine. 
    """

    def __init__(self, array, r):
        self.array = array
        self.r = r
    
    def handle_event(self, event, data):
        """Respond to an incoming event as appropriate.
        """
        log.info(f"{self.array} in state {self.name}, handling new event: {event}")

    def on_entry(self, data):
        """Performs these actions on entry.
        """
        pass

    def on_exit(self):
        """Performs these actions on exit.
        """
        pass

class RecProc(State):
    """State for use with the Record-Process state machine. 
    """
    def __init__(self, array):
        super().__init__(array)
        self.states = {
            "READY":Ready(array)
            "RECORD":Record(array)
        }

class Ready(RecProc):
    """The subarray is in the READY state. 
    """

    def handle_event(self, event, data):
        super().handle_event(event, data)
        if event == "RECORD":
            if redis_util.is_primary_time(self.array):
                return self.states["RECORD_PRIMARY"]
            else:
                return self.states["RECORD"]
        else:
            return self
            
class Record(RecProc):
    """The subarray is in the RECORD state
    """

    def on_entry(self, data):

        subscribed = data["subscribed"]
        ready = data["ready"]

        if ready == subscribed.intersection(ready):
            result = recproc.record(self.r, self.array, list(ready))
            # update data:
            data["recording"] = result
            data["ready"] = ready^result
        else:
            log.error("Not all ready instances are subscribed.")

    def handle_event(self, event, data):
        super().handle_event(event, data)
        if event == "TRACK_STOP":
            log.info(f"{self.array} stopped tracking before DWELL complete")
            redis_util.reset_dwell(self.r, data["recording"], DEFAULT_DWELL)
            return self.states["REC_COMPLETE"]
        elif event == "REC_END"
            return self.states["REC_COMPLETE"]
        else:
            return self