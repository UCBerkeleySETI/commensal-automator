from automator.logger import log
from automator import redis_util, subscription_utils, util
from automator import rec_util as rec

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

class FreeSubscribe(State):
    """State for use with the Free-Subscribe state machine.
    """
    def __init__(self, array):
        super().__init__(array)
        self.states = {
            "SUBSCRIBE":Subscribed(array)
            "FREE":Free(array)
        }


class Free(FreeSubscribe):
    """State in which the subarray is not configured and no nodes are
    subscribed.
    """

    def on_entry(self, data, free):
        """Deallocate instances from a subarray and instruct them to
        unsubscribe from their assigned multicast groups.
        """
        subscription_utils.unsubscribe(self.r, self.array, data["subscribed"])
        while data["subscribed"]:
            free.add(data["subscribed"].pop())

    def handle_event(self, event, free, data):
        super().handle_event(event, data)
        if event == "CONFIGURE":
            if free:
                self.states["SUBSCRIBE"].on_entry(data, free)
                return self.states["SUBSCRIBE"]
            else:
                message = f"No free instances, not configuring {array}"
                redis_util.alert(self.r, message, "coordinator")
                return self
        else:
            return self


class Subscribed(FreeSubscribe):
    """State in which DAQ instances are assigned to a particular
    subarray.
    """

    def on_entry(self, data, free):
        """Allocate instances to a new subarray.
        """

        # Attempt to claim the required number of instances from those that
        # are free:
        n_requested = subscription_utils.num_requested(self.r, self.array)
        while len(free) > 0 and len(data["subscribed"]) < n_requested:
            data["subscribed"].add(free.pop())
        if len(data["subscribed"]) < n_requested:
            message = f"{len(data["subscribed"])}/{n_requested} available."
            redis_util.alert(self.r, message, "coordinator")

        # Initiate subscription process:
        subscription_utils.subscribe(self.r, array, instances)

    def handle_event(self, event, free, data):
        super().handle_event(event, data)
        if event == "DECONFIGURE":
            self.states["FREE"].on_entry(data, free)
            return self.states["FREE"]
        else:
            return self

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
    """The coordinator is in the READY state.
    """

    def on_entry(self, data):
        log.info(f"{self.array} entering READY state")

    def handle_event(self, event, data):
        super().handle_event(event, data)
        if event == "RECORD":
            if redis_util.is_primary_time(self.array):
                self.states["RECORD_PRIMARY"].on_entry(data)
                return self.states["RECORD_PRIMARY"]
            self.states["RECORD"].on_entry(data)
            return self.states["RECORD"]
        else:
            return self

class Record(RecProc):
    """The coordinator is in the RECORD state
    """

    def on_entry(self, data):

        subscribed = data["subscribed"]
        ready = data["ready"]

        if ready == subscribed.intersection(ready):
            result = rec.record(self.r, self.array, list(ready))
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
            if redis_util.is_primary_time(self.array):
                # move them back into the ready state
                while data["recording"]:
                    data["ready"].add(data["recording"].pop())
                return self.states["READY"]
            else:
                self.states["PROCESS"].on_entry(data)
                return self.states["PROCESS"]
        elif event == "REC_END":
            self.states["PROCESS"].on_entry(data)
            return self.states["PROCESS"]
        else:
            return self


class Process(RecProc):
    """The coordinator is in the PROCESS state.
    """

    def on_entry(self, data):
        """Initiate processing on the appropriate processing nodes.
        """

        # Move from recording to processing:
        while data["recording"]:
            data["processing"].add(data["recording"].pop())

        # Use circus to start the processing script for each instance. Note
        # there could be more than one instance per host. Instance names
        # must conform to the following format: <host>/<instance>
        for instance in data["processing"]:
            host, instance_number = instance.split("/")
            util.zmq_circus_cmd(host, f"proc_{instance_number}", "start")


    def handle_event(self, event, data):
        super().handle_event(event, data)
        # If a node completes processing:
        if "RETURN" in event:
            _, instance, returncode = event.split(":")
            if instance in data["processing"]:
                data["processing"].remove(instance)
                data["completed"].add(instance)
                # If all (or whatever preferred percentage) is completed,
                # continue to the next state:
                if(proc_util.check(data)):
                    self.states["READY"].on_entry(data)
                    return self.states["READY"]
            else:
                log.warning(f"Unrecognised instance: {instance}")
        return self
