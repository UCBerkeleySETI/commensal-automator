import redis

from coordinator import util, redis_util
from coordinator.logger import log

from coordinator.states import Ready, Free
from coordinator.state_machines import RecProcMachine, FreeSubscribedMachine

class Coordinator(object):
    """Coordinator that runs on the headnode and allocates instances to
    recording and processing tasks for each subarray.

    The Coordinator is a singleton for all subarrays. 
    """

    def __init__(self, config_file):

        config = util.config(config_file)
        self.channels = config["channels"]
        self.free = set(config["hashpipe_instances"])
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
        #self.alert("Starting up")

        for array in self.arrays:

            # For now, assume we will always start in READY and FREE for each subarray

            self.subscribed[array] = set()

            self.freesubscribed_machines[array] = FreeSubscribedMachine(Free(array, self.r), self.free, self.subscribed[array])
            self.recproc_machines[array] = RecProcMachine(Ready(array, self.r), self.all_instances, self.subscribed[array])

        # Listen for events and respond:

        ps = self.redis_server.pubsub(ignore_subscribe_messages=True)
        ps.subscribe(self.channels)

        for message in ps.listen():
            # TODO: Richer message parsing and review of alerts channel messages.
            # TODO: Decide about how to manage recording_complete messaging
            # (watcher-style process for each instance, or timer in rec_util?)
            components = redis_util.parse_msg(message)
            if components:
                if components[0] == "RETURN":
                    self.processing_return(message)
                else:
                    array = components[1]
                    event = self.message_to_event(components[0])
                    self.freesubscribed_machines[array].state.handle_event(event)
                    self.recproc_machines[array].state.handle_event(event)

    def message_to_event(self, message):
        """Convert an incoming message into an event transition.
        """
        if message == "configure":
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

    def processing_return(self, message):
        """Note, we must return these to every array's state machine for the
        moment until we start using Redis hashes for instance-specific
        communication.
        """
        for machine in self.recproc_machines.values():
            machine.state.handle_event(message)

    def alert(self, message):
        redis_util.alert(self.r, message, "[test] coordinator")
    
    def annotate(self, tag, text):
        response = util.annotate_grafana(tag, text)
        log.info(f"Annotating Grafana, response: {response}")
