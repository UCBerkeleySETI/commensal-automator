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
         self.state = current_state.handle_event(event, data)


class RecProcMachine(object):

    def __init__(self, initial_state, all_instances, subscribed):

        self.state = initial_state
        
        self.data = {
            "subscribed":subscribed,
            "ready":set(),
            "recording":set(),
            "processing":set()
        }

        self.data[initial_state.name] = all_instances 

    def handle_event(self, event):
         self.state = current_state.handle_event(event, data)