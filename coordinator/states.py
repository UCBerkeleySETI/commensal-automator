import time

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
        redis_util.alert(self.r,
            f":magic_wand: `{self.array}` configuring",
            "coordinator")
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
        self.primary_time = False
        self.timer = None

    def on_entry(self, data):

        log.info(f"{self.array} entering state: {self.name}")
        subscribed = data["subscribed"]
        ready = data["ready"]
        if subscribed.issubset(ready) and subscribed:
            result = rec.record(self.r, self.array, list(subscribed))
            if result:
                # update data:
                data["recording"] = result["instances"]
                data["ready"] = ready.difference(result)
                # add timer:
                self.timer = result["timer"]
                # check primary time:
                if rec.check_primary_time(self.r, self.array):
                    self.primary_time = True
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
            # Reset stop time. Note, this is not calculated directly from
            # PKTIDX, so use this value accordingly.
            datadir = self.r.get(f"{self.array}:datadir")
            self.r.set(f"rec_end:{datadir}", time.time())
            # Alert if too short
            if not proc_util.check_length(self.r, datadir, 150):
                redis_util.alert(self.r, f":timer_clock: `{datadir}` too short, ignoring",
                    "coordinator")
            # Cancel timer:
            if not self.timer:
                redis_util.alert(self.r, f":warning: `{self.array}` no timer for `{datadir}`",
                    "coordinator")
            else:
                self.timer.cancel()
            # End recording early:
            redis_util.reset_dwell(self.r, data["recording"], DEFAULT_DWELL)
            redis_util.alert(self.r,
                f":black_square_for_stop: `{self.array}` recording stopped",
                "coordinator")
            if self.primary_time:
                # move them back into the ready state
                while data["recording"]:
                    data["ready"].add(data["recording"].pop())
                return Waiting(self.array, self.r)
            return Process(self.array, self.r)
        elif event == "REC_END":
            redis_util.alert(self.r,
                f":black_square_for_stop: `{self.array}` recording ended",
                "coordinator")
            if self.primary_time:
                # move them back into the ready state
                while data["recording"]:
                    data["ready"].add(data["recording"].pop())
                return Waiting(self.array, self.r)
            return Process(self.array, self.r)
        else:
            return self


class Process(State):
    """The coordinator is in the PROCESS state.
    """

    def __init__(self, array, r):
        super().__init__(array, r)
        self.name = "PROCESS"
        self.returncodes1 = []
        self.returncodes2 = []

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

        n = proc_util.get_n_proc(self.r)
        if n%10 == 0:
            # Alert will write filterbank files:
            redis_util.alert(self.r,
                f":potable_water: `{self.array}` will save beamformer output",
                "coordinator")
            # Alert will attempt experimental ML detection:
            redis_util.alert(self.r,
                f":test_tube: `{self.array}` running ML experiment",
                "coordinator")

        # Grafana tag:
        util.annotate_grafana("PROCESS", f"{self.array}: processing")

        return True


    def handle_event(self, event, data):
        super().handle_event(event, data)
        # If a node completes processing:
        if "RETURN" in event:
            _, instance, returncode1, returncode2 = event.split(":")
            log.info(f"{_} {instance} {returncode1} {returncode2}")
            if instance in data["processing"]:
                data["processing"].remove(instance)
                data["ready"].add(instance)
                self.returncodes1.append(int(returncode1))
                self.returncodes2.append(int(returncode2))
                # If all (or whatever preferred percentage) is completed,
                # continue to the next state:
                if not data["processing"]:
                    codes1 = proc_util.output_summary(self.returncodes1)
                    codes2 = proc_util.output_summary(self.returncodes2)

                    log.info(self.returncodes1)

                    if max(self.returncodes2) < 0:
                        stage2_msg = None
                    elif max(self.returncodes2) < 1:
                        stage2_msg = f":white_check_mark: `{self.array}` stage 2 complete: {codes2}"
                    elif max(self.returncodes2) < 2:
                        stage2_msg = f":heavy_check_mark: `{self.array}` stage 2 complete: {codes2}"
                    else:
                        stage2_msg = f":warning: `{self.array}` stage 2 complete: {codes2}"

                    if max(self.returncodes1) < 1:
                        redis_util.alert(self.r,
                            f":white_check_mark: `{self.array}` stage 1 complete: {codes1}",
                            "coordinator")
                        if stage2_msg:
                            redis_util.alert(self.r, stage2_msg, "coordinator")
                        proc_util.increment_n_proc(self.r)
                        self.returncodes1 = []
                        self.returncodes2 = []
                        return Ready(self.array, self.r)
                    # Check and clear the returncodes:
                    elif max(self.returncodes1) < 2:
                        redis_util.alert(self.r,
                            f":heavy_check_mark: `{self.array}` stage 1 complete: {codes1}",
                            "coordinator")
                        if stage2_msg:
                            redis_util.alert(self.r, stage2_msg, "coordinator")
                        proc_util.increment_n_proc(self.r)
                        self.returncodes1 = []
                        self.returncodes2 = []
                        return Ready(self.array, self.r)
                    else:
                        redis_util.alert(self.r,
                            f":warning: `{self.array}`: {codes1}",
                            "coordinator")
                        return Error(self.array, self.r)
            else:
                log.warning(f"Unrecognised instance: {instance}")
        return self


class Waiting(State):
    """Wait in this state for further human intervention.
    """
    def __init__(self, array, r):
        super().__init__(array, r)
        self.name = "WAITING"

    def on_entry(self, data):
        log.info(f"{self.array} entering state: {self.name}")
        redis_util.alert(self.r,
            f":bust_in_silhouette: `{self.array}` intervention required",
            "coordinator")
        return True

    def handle_event(self, event, data):
        super().handle_event(event, data)
        log.info(f"In WAITING state, therefore ignoring: {event}")
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