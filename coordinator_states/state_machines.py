"""
State machine class for use with the coordinator.
"""


class FreeSubscribedMachine(object):

    def __init__(self, initial_state, free, subscribed):

        self.state = initial_state

        self.data = {
            "free":free,
            "subscribed":subscribed
        }

    def handle_event(self, event):
        new_state = self.state.handle_event(event, self.data)
        # Run on_entry only if we are entering a new state
        if new_state.name != self.state.name:
            self.state = new_state
            self.state.on_entry(self.data)


class RecProcMachine(object):

    def __init__(self, initial_state, all_instances, subscribed):

        self.state = initial_state

        self.data = {
            "subscribed":subscribed,
            "ready":set(),
            "recording":set(),
            "processing":set()
        }

        self.data[initial_state.name] = all_instances.copy()

    def handle_event(self, event):
        new_state = self.state.handle_event(event, self.data)
        # Run on_entry only if we are entering a new state
        if new_state.name != self.state.name:
            self.state = new_state
            self.state.on_entry(self.data)