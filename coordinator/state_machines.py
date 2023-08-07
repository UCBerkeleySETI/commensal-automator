from coordinator.logger import log
from coordinator import redis_util

class FreeSubscribedMachine(object):
    """State machine to handle subscribing and unsubscribing from
    multicast groups.
    """
    def __init__(self, initial_state, free, subscribed, r):

        self.r = r

        self.state = initial_state

        self.data = {
            "free":free,
            "subscribed":subscribed
        }

    def handle_event(self, event):
        new_state = self.state.handle_event(event, self.data)
        # Run on_entry only if we are entering a new state
        if new_state.name != self.state.name:
            # Attempt to enter the new state:
            if new_state.on_entry(self.data):
                self.state = new_state
            else:
                # stay in current state if entry failed
                log.warning(f"Could not enter new state: {new_state.name}")
        # Save the current state:
        redis_util.save_free(self.data["free"], self.r)



class RecProcMachine(object):
    """State machine to handle recording, processing and cleanup.
    """
    def __init__(self, initial_state, all_instances, subscribed, r):

        self.r = r

        self.state = initial_state

        self.data = {
            "subscribed":subscribed,
            "ready":set(),
            "recording":set(),
            "processing":set(), 
        }

        # For now, we always start with all instances in "ready"
        self.data["ready"] = all_instances.copy()

    def handle_event(self, event):
        new_state = self.state.handle_event(event, self.data)
        # Run on_entry only if we are entering a new state
        if new_state.name != self.state.name:
            # Attempt to enter the new state:
            if new_state.on_entry(self.data):
                self.state = new_state
            else:
                # stay in current state if entry failed
                log.warning(f"Could not enter new state: {new_state.name}")