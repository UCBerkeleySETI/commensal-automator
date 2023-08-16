from coordinator.logger import log
from coordinator import redis_util

class FreeSubscribedMachine(object):
    """State machine to handle subscribing and unsubscribing from
    multicast groups.
    """

    def __init__(self, initial_state, data, r):
        self.r = r
        self.state = initial_state
        self.data = data

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
        # Save the current free instances:
        redis_util.save_free(self.data["free"], self.r)

class RecProcMachine(object):
    """State machine to handle recording, processing and cleanup.
    """

    def __init__(self, initial_state, data, r):
        self.r = r
        self.state = initial_state
        self.data = data

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

        # If any instances are subscribed, the freesub state machine is in the
        # SUBSCRIBED state.
        if self.data["subscribed"]:
            freesub_state = "SUBSCRIBED"
        else:
            freesub_state = "FREE"

        # Assemble and save state data
        state_data = {
            "freesub_state":freesub_state,
            "recproc_state":self.state.name,
            "subscribed":list(self.data["subscribed"]),
            "ready":list(self.data["ready"]),
            "recording":list(self.data["recording"]),
            "processing":list(self.data["processing"])
            }

        redis_util.save_state(self.state.array, state_data, self.r)