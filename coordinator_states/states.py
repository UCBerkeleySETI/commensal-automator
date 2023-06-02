from automator.logger import log


class State(object):
    """State object for use with the coordinator state machine. 
    """

    def __init__(self, array):
        self.array = array
    
    def handle_event(self, event, data):
        """Respond to an incoming event as appropriate.
        """
        log.info(f"{self.array} in state {self.name}, handling new event: {event}")

    def on_entry(self, event, data):
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
        suoer().__init__(array)
        self.states = {
            "READY":Ready(array)
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
            



