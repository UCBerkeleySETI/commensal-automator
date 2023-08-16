import redis

from coordinator import util, redis_util
from coordinator.logger import log

from coordinator.states import Ready, Record, Process, Error, Free, Subscribed
from coordinator.state_machines import RecProcMachine, FreeSubscribedMachine

class Coordinator(object):
    """Coordinator that runs on the headnode and allocates instances to
    recording and processing tasks for each subarray.

    The Coordinator is a singleton for all subarrays. 
    """

    def __init__(self, config_file):

        config = util.config(config_file)
        self.channels = config["channels"]
        self.free = set(config["hashpipe_instances"]) # default instance list
        self.all_instances = set(config["hashpipe_instances"].copy()) # is copy() needed here?
        self.arrays = config["arrays"]
        self.r = redis.StrictRedis(host=config["redis_host"],
                                   port=config["redis_port"],
                                   decode_responses=True)
        self.recproc_machines = dict()
        self.freesubscribed_machines = dict()
        self.subscribed = dict()

    def start(self):
        """Start the coordinator.
        """
        redis_util.alert(self.r,
            f":large_green_circle: starting up...",
            "coordinator")

        # Check if a list of FREE instances is available in Redis. This set
        # is shared by all subarray state machines and so must be initialised
        # here.
        free = redis_util.read_free(self.r)
        if free:
            self.free = set(free)

        # Look for saved state data when starting
        for array in self.arrays:
            state_data = redis_util.read_state(array, self.r)
            if state_data:
                recproc_state = state_data["recproc_state"]
                freesub_state = state_data["freesub_state"]
                # Set of subscribed nodes is shared between both machines.
                self.subscribed[array] = set(state_data["subscribed"])
                recproc_data = {
                     "subscribed":self.subscribed[array],
                     "ready":set(state_data["ready"]),
                     "recording":set(state_data["recording"]),
                     "processing":set(state_data["processing"]),
                     }
                freesub_data = {
                    "free":self.free,
                    "subscribed":self.subscribed[array]
                 }
            # If no saved data:
            else:
                recproc_state = "READY"
                freesub_state = "FREE"
                self.subscribed[array] = set()
                recproc_data = {
                    "subscribed":self.subscribed[array],
                    "ready":set(self.all_instances.copy()),
                    "recording":set(),
                    "processing":set(),
                    }
                freesub_data = {
                    "free":self.free,
                    "subscribed":self.subscribed[array] # same object
                }

            # Create the initial states:
            recproc_init = self.create_state(recproc_state, array, self.r)
            freesub_init = self.create_state(freesub_state, array, self.r)

            # Create the new state machines:
            self.freesubscribed_machines[array] = FreeSubscribedMachine(freesub_init, freesub_data, self.r)
            self.recproc_machines[array] = RecProcMachine(recproc_init, recproc_data, self.r)

        # Listen for events and respond:
        ps = self.r.pubsub(ignore_subscribe_messages=True)
        ps.subscribe(self.channels)

        for message in ps.listen():
            components = redis_util.parse_msg(message)
            if components:
                if components[0] == "RETURN":
                    self.processing_return(message["data"])
                else:
                    array = components[1]
                    if array not in self.freesubscribed_machines:
                        log.warning(f"Unrecognised array key: {array}")
                        continue
                    event = self.message_to_event(components[0])
                    self.freesubscribed_machines[array].handle_event(event)
                    self.recproc_machines[array].handle_event(event)

    def message_to_event(self, message):
        """Convert an incoming message into an event transition.
        """
        log.info(message)
        if message == "conf_complete":
            return "CONFIGURE"
        elif message == "deconfigure":
            return "DECONFIGURE"
        elif message == "tracking":
            return "RECORD"
        elif message == "not-tracking":
            return "TRACK_STOP"
        elif message == "rec-timeout":
            return "REC_END"
        else:
            return message

    def create_state(self, name, array, r):
        """Return a new state object with the given parameters.
        """
        states = {
            "FREE":Free,
            "SUBSCRIBED":Subscribed,
            "READY":Ready,
            "RECORD":Record,
            "PROCESS":Process,
            "ERROR":Error,
            }
        state = states.get(name)
        if state:
            return state(array, r)
        else:
            log.error(f"Unrecognised state: {name}")

    def processing_return(self, message):
        """Note, we must return these to every array's state machine for the
        moment until we start using Redis hashes for instance-specific
        communication.
        """
        for machine in self.recproc_machines.values():
            machine.handle_event(message)
    
    def annotate(self, tag, text):
        """Add an annotation to Grafana plots to be viewed alongside e.g.
        the 40GbE heatmap.
        """
        response = util.annotate_grafana(tag, text)
        log.info(f"Annotating Grafana, response: {response}")
