from coordinator.logger import log
from coordinator import redis_util, sub_util, util, proc_util
from coordinator import rec_util as rec

DEFAULT_DWELL = 290 # in seconds

class State(object):
    """State object for use with the coordinator state machine. 
    """

    def __init__(self, array, r):
        self.array = array
        self.r = r
        self.name = "NEW_STATE"

    def handle_event(self, event, data):
        """Respond to an incoming event as appropriate.
        """
        log.info(f"{self.name} handling new event: {event} for {self.array}")


class Free(State):
    """State in which the subarray is not configured and no nodes are
    subscribed.
    """

    def __init__(self, array, r):
        super().__init__(array, r)
        self.name = "FREE"

    def on_entry(self, data):
        """Deallocate instances from a subarray and instruct them to
        unsubscribe from their assigned multicast groups.
        """
        log.info(f"{self.array} entering state: {self.name}")
        if data["subscribed"]:
            sub_util.unsubscribe(self.r, self.array, data["subscribed"])
            while data["subscribed"]:
                data["free"].add(data["subscribed"].pop())
        return True

    def handle_event(self, event, data):
        super().handle_event(event, data)
        if event == "CONFIGURING":
            return Configuring(self.array, self.r)
        return self

class Configuring(State):
    """Enter this state when awaiting the arrival of metadata.
    """

    def __init__(self, array, r):
        super().__init__(array, r)
        self.name = "CONFIGURING"

    def on_entry(self, data):
        """Wait for metadata to arrive before entering SUBSCRIBED state.
        """
        log.info(f"{self.array} entering state: {self.name}")
        return True

    def handle_event(self, event, data):
        super().handle_event(event, data)
        if event == "CONFIGURED":
            if data["free"]:
                return Subscribed(self.array, self.r)
            else:
                # If there are no available hosts, return to FREE
                message = f":no_entry_sign: `{self.array}` no free instances, not configuring."
                redis_util.alert(self.r, message, "coordinator")
                return Free(self.array, self.r)
        elif event == "DECONFIGURE":
            return Free(self.array, self.r)
        return self


class Subscribed(State):
    """State in which DAQ instances are assigned to a particular
    subarray.
    """

    def __init__(self, array, r):
        super().__init__(array, r)
        self.name = "SUBSCRIBED"

    def on_entry(self, data):
        """Allocate instances to a new subarray.
        """
        log.info(f"{self.array} entering state: {self.name}")

        # Attempt to claim the required number of instances from those that
        # are free:
        n_requested = sub_util.num_requested(self.r, self.array)
        free_instances = redis_util.sort_instances(list(data["free"]))
        while len(data["free"]) > 0 and len(data["subscribed"]) < n_requested:
            data["free"].remove(free_instances[0])
            data["subscribed"].add(free_instances.pop(0))
        if len(data["subscribed"]) < n_requested:
            n_subs = len(data["subscribed"])
            message = f":warning: `{self.array}` {n_subs}/{n_requested} available."
            redis_util.alert(self.r, message, "coordinator")

        # Initiate subscription process:
        sub_util.subscribe(self.r, self.array, data["subscribed"])
        return True

    def handle_event(self, event, data):
        super().handle_event(event, data)
        if event == "DECONFIGURE":
            return Free(self.array, self.r)
        return self


class Ready(State):
    """The coordinator is in the READY state.
    """

    def __init__(self, array, r):
        super().__init__(array, r)
        self.name = "READY"

    def on_entry(self, data):
        log.info(f"{self.array} entering state: {self.name}")
        return True

    def handle_event(self, event, data):
        super().handle_event(event, data)
        if event == "RECORD":
            return Record(self.array, self.r)
        return self

class Record(State):
    """The coordinator is in the RECORD state
    """

    def __init__(self, array, r):
        super().__init__(array, r)
        self.name = "RECORD"

    def on_entry(self, data):

        log.info(f"{self.array} entering state: {self.name}")

        subscribed = data["subscribed"]
        ready = data["ready"]
        if subscribed == subscribed.intersection(ready):
            result = rec.record(self.r, self.array, list(ready))
            if result:
                # update data:
                data["recording"] = result
                data["ready"] = ready.difference(result)
                return True
            log.warning("Could not start recording.")
            return False
        else:
            log.error("Not all ready instances are subscribed.")
            return False

    def handle_event(self, event, data):
        super().handle_event(event, data)
        if event == "TRACK_STOP":
            log.info(f"{self.array} stopped tracking before DWELL complete")
            redis_util.reset_dwell(self.r, data["recording"], DEFAULT_DWELL)
            redis_util.alert(self.r,
                f":black_square_for_stop: `{self.array}` recording stopped",
                "coordinator")
            if redis_util.is_primary_time(self.r, self.array):
                # move them back into the ready state
                while data["recording"]:
                    data["ready"].add(data["recording"].pop())
                return Ready(self.array, self.r)
            else:
                return Process(self.array, self.r)
        elif event == "REC_END":
            redis_util.alert(self.r,
                f":black_square_for_stop: `{self.array}` recording ended",
                "coordinator")
            return Process(self.array, self.r)
        else:
            return self


class Process(State):
    """The coordinator is in the PROCESS state.
    """

    def __init__(self, array, r):
        super().__init__(array, r)
        self.name = "PROCESS"
        self.returncodes = []

    def on_entry(self, data):
        """Initiate processing on the appropriate processing nodes.
        """

        log.info(f"{self.array} entering state: {self.name}")

        # Move from recording to processing:
        while data["recording"]:
            data["processing"].add(data["recording"].pop())

        # Use circus to start the processing script for each instance. Note
        # there could be more than one instance per host. Instance names
        # must conform to the following format: <host>/<instance>
        for instance in data["processing"]:
            host, instance_number = instance.split("/")
            proc = util.zmq_circus_cmd(host, f"bluse_analyzer_{instance_number}", "start")
            if not proc:
                log.error(f"Could not start processing on {instance}")

        # Alert processing
        redis_util.alert(self.r,
            f":gear: `{self.array}` processing",
            "coordinator")

        # Grafana tag:
        util.annotate_grafana("PROCESS", f"{self.array}: processing")

        return True


    def handle_event(self, event, data):
        super().handle_event(event, data)
        # If a node completes processing:
        if "RETURN" in event:
            _, instance, returncode = event.split(":")
            if instance in data["processing"]:
                data["processing"].remove(instance)
                data["ready"].add(instance)
                self.returncodes.append(int(returncode))
                # If all (or whatever preferred percentage) is completed,
                # continue to the next state:
                if not data["processing"]:
                    if max(self.returncodes) < 1:
                        redis_util.alert(self.r,
                            f":white_check_mark: `{self.array}` complete, code 0",
                            "coordinator")
                        proc_util.increment_n_proc(self.r)
                        self.returncodes = []
                        return Ready(self.array, self.r)
                    # Check and clear the returncodes:
                    elif max(self.returncodes) < 2:
                        redis_util.alert(self.r,
                            f":heavy_check_mark: `{self.array}` complete, codes: `{self.returncodes}`",
                            "coordinator")
                        proc_util.increment_n_proc(self.r)
                        self.returncodes = []
                        return Ready(self.array, self.r)
                    else:
                        return Error(self.array, self.r)
            else:
                log.warning(f"Unrecognised instance: {instance}")
        return self


class Error(State):
    """Error state for the record-process state machine.

    Leaving the error state requires manual intervention.
    """

    def __init__(self, array, r):
        super().__init__(array, r)
        self.name = "ERROR"

    def on_entry(self, data):
        log.info(f"{self.array} entering state: {self.name}")
        redis_util.alert(self.r,
            f":x: `{self.array}` ERROR",
            "coordinator")
        return True

    def handle_event(self, event, data):
        super().handle_event(event, data)
        log.info(f"In ERROR state, therefore ignoring: {event}")
        return self