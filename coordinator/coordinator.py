import redis

from coordinator import util, redis_util
from coordinator.logger import log

from coordinator.states import Ready, Record, Process, Error, Free, Subscribed, Configuring, Waiting
from coordinator.state_machines import RecProcMachine, FreeSubscribedMachine

class Coordinator(object):
    """Coordinator that runs on the headnode and allocates instances to
    recording and processing tasks for each subarray.

    The Coordinator is a singleton for all subarrays. 
    """

    def __init__(self, config_file):

        self.r = redis.StrictRedis(decode_responses=True)
        config = util.config(self.r, config_file)
        self.channels = config["channels"]
        self.free = set(config["hashpipe_instances"]) # default instance list
        self.all_instances = set(config["hashpipe_instances"].copy()) # is copy() needed here?
        self.arrays = config["arrays"]
        self.recproc_machines = dict()
        self.freesubscribed_machines = dict()
        self.subscribed = dict()

    def start(self):
        """Start the coordinator.
        """
        redis_util.alert(self.r,
            f":large_green_circle: starting up...",
            "coordinator")

        self.initialise_machines()

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


    def initialise_machines(self):
        """Initialise all states and state machines for the specified array.
        Checks if any prior state information was stored in Redis, and uses
        that to initialise the state machines and new states.
        """

        # Check if a list of FREE instances is available in Redis. This set
        # is shared by all subarray state machines and so must be initialised
        # here.
        free = redis_util.read_free(self.r)
        if free:
            self.free = set(free)

        # Look for saved state data for each array:
        for array in self.arrays:
            # recproc and freesub data; reproc state:
            recproc_state_data = redis_util.read_state(array, self.r)
            if recproc_state_data:
                recproc_state = recproc_state_data["recproc_state"]
                # Set of subscribed nodes is shared between both machines.
                self.subscribed[array] = set(recproc_state_data["subscribed"])
                recproc_data = {
                        "subscribed":self.subscribed[array],
                        "ready":set(recproc_state_data["ready"]),
                        "recording":set(recproc_state_data["recording"]),
                        "processing":set(recproc_state_data["processing"]),
                        }
                freesub_data = {
                    "free":self.free,
                    "subscribed":self.subscribed[array]
                    }
            # If no saved data:
            else:
                recproc_state = "READY"
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

            # freesub state:
            freesub_state = redis_util.read_freesub_state(array, self.r)
            # If freesub state is not saved in Redis
            if not freesub_state:
                if self.subscribed[array]:
                    freesub_state = "SUBSCRIBED"
                else:
                    freesub_state = "FREE"

            # Create the initial states:
            recproc_init = self.create_state(recproc_state, array, self.r)
            freesub_init = self.create_state(freesub_state, array, self.r)

            # Create the new state machines:
            self.freesubscribed_machines[array] = FreeSubscribedMachine(freesub_init, freesub_data, self.r)
            self.recproc_machines[array] = RecProcMachine(recproc_init, recproc_data, self.r)


    def message_to_event(self, message):
        """Convert an incoming message into an event transition.
        """
        log.info(message)
        if message == "configure":
            return "CONFIGURING"
        if message == "conf_complete":
            return "CONFIGURED"
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
            "CONFIGURING":Configuring,
            "READY":Ready,
            "RECORD":Record,
            "PROCESS":Process,
            "WAITING":Waiting,
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
