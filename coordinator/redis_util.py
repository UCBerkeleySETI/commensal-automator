#!/usr/bin/env python
# Helper functions for looking up various redis data.
# The convention is that "r" is our redis client.

from datetime import datetime, timezone
import os
import re
import redis
import sys
import time
import numpy as np
import json

from coordinator.logger import log

SLACK_CHANNEL = "meerkat-obs-log"
SLACK_PROXY_CHANNEL = "slack-messages"
PROPOSAL_ID = 'EXT-20220504-DM-01' 

def sort_instances(instances):
    """Sort the instances by host and instance number.
    Accepts instances (list). Format as follows:
    ["blpn0/0", "blpn0/1", "blpn1/0", ... ]
    """
    return sorted(instances, key = lambda x: (int(x[4:x.index("/")]), int(x[x.index("/")+1:])))

def save_free(free, r):
    """Save the set of globally available, unassigned instances.
    """
    log.info(f"Saving free instances: {free}")
    r.set("free_instances", json.dumps(list(free)))

def save_freesub_state(array, state, r):
    """Save the current freesub state name.
    """
    log.info(f"Saving freesub state: {state}")
    r.set(f"{array}:freesub_state", state)

def read_freesub_state(array, r):
    """Read the most recent freesub state name.
    """
    state = r.get(f"{array}:freesub_state")
    log.info(f"Reading freesub state: {state}")
    return state

def read_free(r):
    """Retrieve the set of globally available, unassigned instances.
    """
    free = r.get("free_instances")
    log.info(f"Loading free instances: {free}")
    if free:
        return set(json.loads(free))
    return None

def save_state(array, data, r):
    """Write or update the current state for the specified array into Redis.
     machine = state machine

    Expected structure for `data`:

    {
        "recproc_state": <state>,
        "subscribed": <list of instances>,
        "ready": <list of instances>,
        "recording": <list of instances>,
        "processing": <list of instances>,
        "timestamp": <timestamp of data>
    }
    """
    log.info(f"Updating {array} state, data:")
    log.info(f"{data}")
    array_key = f"{array}:state"
    r.set(array_key, json.dumps(data))

def read_state(array, r):
    """Read state and associated information if available.
    """
    state_data = r.get(f"{array}:state")
    if state_data:
        log.info(f"Loading previous state data for {array}")
        log.info(state_data)
        return json.loads(state_data)
    else:
        log.info(f"No saved state data for {array}")
        return

def raw_files(r):
    """Returns a dict mapping host name to a list of raw files on the host."""
    hosts = [key.split(":")[-1] for key in r.keys("bluse_raw_watch:*")]
    pipe = r.pipeline()
    for host in hosts:
        pipe.smembers("bluse_raw_watch:" + host)
    results = pipe.execute()
    answer = {}
    for host, result in zip(hosts, results):
        answer[host] = sorted(result)
    return answer


def sb_id(r, subarray):
    """Returns the current schedule block id, in hyphenated form.
    For example, it could be:
    20221128-0003
    Raises a ValueError if there is none.
    May return "Unknown_SB" if the upstream code indicates the schedule block is unknown.
    """
    sb_id_list = r.get(f"{subarray}:sched_observation_schedule_1")
    if not sb_id_list:
        raise ValueError("no schedule block ids found")
    answer = sb_id_list.split(",")[0]
    if answer == "Unknown_SB":
        return answer
    if not re.match(r"[0-9]{8}-[0-9]{4}", answer):
        raise ValueError(f"bad sb_id_list value: {sb_id_list}")
    return answer

def get_bluse_dwell(r, subarray_name):
    """Get specified dwell for BLUSE primary time observing.
    Dwell in seconds.
    """
    bluse_dwell_key = f"bluse_dwell:{subarray_name}"
    bluse_dwell = r.get(bluse_dwell_key)
    if bluse_dwell is None:
       log.warning('No specified primary time DWELL; defaulting to 290 sec.')
       bluse_dwell = 290   
    return int(bluse_dwell)

def reset_dwell(r, instances, dwell):
    """Reset DWELL for given list of instances.
    """
    chan_list = channel_list('bluse', instances)
    # Send messages to these specific hosts:
    log.info(f"Resetting DWELL for {instances}, new dwell: {dwell}")
    for i in range(len(chan_list)):
        r.publish(chan_list[i], "DWELL=0")
        r.publish(chan_list[i], "PKTSTART=0")

    # Wait for processing nodes:
    time.sleep(1.5)

    # Reset DWELL
    for i in range(len(chan_list)):
        r.publish(chan_list[i], f"DWELL={dwell}")

def channel_list(hpgdomain, instances):
    """Build a list of Hashpipe-Redis Gateway channels from a list
       of instance names (of format: host/instance)
    """
    return [f"{hpgdomain}://{instance}/set" for instance in instances]

def is_primary_time(r, subarray_name):
    """Check if the current (or most recent) observation ID is for BLUSE
    primary time.
    """
    subarray = f"subarray_{subarray_name[-1]}"
    p_id_key = f"{subarray}_script_proposal_id"
    p_id = r.get(f"{subarray_name}:{p_id_key}")
    if p_id == PROPOSAL_ID:
        log.info(f"BLUSE proposal ID detected for {subarray_name}")
        return True
    return False

def get_last_rec_bluse(r, subarray_name):
    """Return True if last recording was made under the BLUSE
    proposal ID (thus primary time). 
    """
    key = f"{subarray_name}:last_rec_primary_t"
    val = r.get(key)
    try:
        val = int(val)
    except:
        log.error(f"Could not determine primary time status: {val}")
    return val 

def set_last_rec_bluse(r, subarray_name, value):
    """Set the value (bool) of the last recording proposal
    ID flag (True if BLUSE ID, False if not).
    """ 
    key = f"{subarray_name}:last_rec_primary_t"
    log.info(f"setting primary time status for {subarray_name}: {value}")
    r.set(key, value)

def primary_sequence_end(r, subarray):
    """If the previously recorded track was a primary time track, and the
    current track is not a primary time track, return True.
    """
    if get_last_rec_bluse(r, subarray) and not is_primary_time(r, subarray):
        return True
    return False

def get_n_proc(r):
    """Retrieve the absolute number of times processing has been run.
    """
    n_proc = r.get("automator:n_proc")
    if n_proc is None:
        r.set("automator:n_proc", 0)
        return 0
    return int(n_proc)

def increment_n_proc(r):
    """Add 1 to the number of times processing has been run.
    """
    n_proc = get_n_proc(r)
    r.set("automator:n_proc", n_proc + 1)

def all_hosts(r):
    return sorted(key.split("//")[-1].split("/")[0]
                  for key in r.keys("bluse://*/0/status"))

def multicast_subscribed(r):
    """Returns a list of the hosts that are subscribed to F-engine multicast groups.
    """
    return [host for (host, destip) in  get_status(r, "bluse", "DESTIP")
            if destip != "0.0.0.0"]

def multiget_status(r, domain, keys):
    """Fetches status from hashpipe processes.
    domain is "bluse" or "blproc".

    Returns a list of (host, value-list) tuples.
    """
    hosts = all_hosts(r)
    pipe = r.pipeline()
    for host in hosts:
        redis_key = f"{domain}://{host}/0/status"
        pipe.hmget(redis_key, keys)
    results = pipe.execute()
    return list(zip(hosts, results))

def multiget_by_instance(r, domain, instances, keys):
    """Fetches status from hashpipe processes.
    domain is "bluse" or "blproc".

    Returns a list of (instance, value-list) tuples.
    """
    pipe = r.pipeline()
    for instance in instances:
        redis_key = f"{domain}://{instance}/status"
        pipe.hmget(redis_key, keys)
    results = pipe.execute()
    return list(zip(instances, results))

def get_status(r, domain, key):
    """Like multiget_status but just one key.

    Returns a list of (host, value) tuples.
    """
    return [(host, results[0]) for (host, results) in multiget_status(r, domain, [key])]

def broken_daqs(r):
    """Return a sorted list of which daqs look broken."""
    answer = []
    for host, result in get_status(r, "bluse", "DAQPULSE"):
        if result is None:
            continue
        delta = datetime.strptime(result, "%c") - datetime.now()
        if abs(delta.total_seconds()) > 60:
            answer.append(host)
    return answer

def get_recording(r):
    """Returns a sorted list of all hosts that are currently recording.
    """
    hkeys = ["PKTIDX", "PKTSTART", "PKTSTOP"]
    for _ in range(10):
        try:
            answer = set()
            for host, strkeys in multiget_status(r, "bluse", hkeys):
                if strkeys[1] == "0":
                    # PKTSTART=0 indicates not-in-use even if the other keys are absent
                    continue
                for k, val in zip(hkeys, strkeys):
                    if val is None:
                        log.warning(f"on host {host} the key {k} is not set")
                        raise IOError("synthetic error to invoke retry logic")
                if None in strkeys:
                    # This is a race condition and we don't know what it means.
                    # Let's treat it as "in use"
                    answer.add(host)
                    continue
                pktidx, pktstart, pktstop = map(int, strkeys)
                if pktstart > 0 and pktidx < pktstop:
                    answer.add(host)
            return sorted(answer)
        except IOError:
            time.sleep(1)
            continue
    raise IOError("get_recording failed even after retry")

def sb_id_from_filename(filename):
    """Works on either raw file names or directory names.
    Returns None if the filename doesn't fit the pattern.
    """
    parts = filename.strip("/").split("/")
    if len(parts) < 3:
        return None
    x, y = parts[1:3]
    if len(x) != 8 or not x.isnumeric() or not y.isnumeric():
        return None
    return f"{x}/{y}"

def gateway_msg(r, channel, msg_key, msg_val, write=True):
    """Format and publish a hashpipe-Redis gateway message. Save messages
    in a Redis hash for later use by reconfig tool.

    Args:
        r: Redis server.
        channel (str): Name of channel to be published to.
        msg_key (str): Name of key in status buffer.
        msg_val (str): Value associated with key.
        write (bool): If true, also write message to Redis database.
    """
    msg = f"{msg_key}={msg_val}"
    listeners = r.publish(channel, msg)
    log.info(f"Published {msg} to channel {channel}")
    # save hash of most recent messages
    if write:
        r.hset(channel, msg_key, msg_val)
        log.info(f"Wrote {msg} for channel {channel} to Redis")
    return listeners

def create_array_groups(r, instances, array, domain="bluse"):
    """Create appropriate groups for a specific array to address subgroups of
    instances (for the case where more than one instance exist on the same
    host).
    """
    for instance in instances:
        host, instance_number = instance.split("/")
        group_name = f"{array}-{instance_number}"
        gateway_channel = f"{domain}://{instance}/gateway"
        message = f"join={group_name}"
        listener = r.publish(gateway_channel, message)
        if listener == 0:
            alert(r,
            f":warning: `{array}`: {instance} did not join {group_name}",
            "coordinator")
        log.info(f"Instance {instance} joining {group_name}")

def destroy_array_groups(r, array, domain="bluse", inst_nums=[0,1]):
    """Instruct all participants in gateway groups associated with `array` to
    leave.
    """
    for n in inst_nums:
        group_name = f"{array}-{n}"
        message = f"leave={group_name}"
        group_gateway_channel = f"{domain}:{group_name}///gateway"
        r.publish(group_gateway_channel, message)
        log.info(f"Instances instructed to leave the gateway group: {group_name}")


def join_gateway_group(r, instances, group_name, gateway_domain):
    """Instruct hashpipe instances to join a hashpipe-redis gateway group.
    
    Hashpipe-redis gateway keys can be published for all these nodes
    simultaneously by publishing to the Redis channel:
    <gateway_domain>:<group_name>///set
    """
    # Instruct each instance to join specified group:
    for instance in instances:
        node_gateway_channel = f"{gateway_domain}://{instance}/gateway"
        msg = f"join={group_name}"
        r.publish(node_gateway_channel, msg)
    log.info(f"Instances {instances} instructed to join gateway group: {group_name}")


def leave_gateway_group(r, group_name, gateway_domain):
    """Instruct hashpipe instances to leave a hashpipe-redis gateway group.
    """
    message = f"leave={group_name}"
    publish_gateway_message(r, group_name, gateway_domain, message)
    log.info(f"Instances instructed to leave the gateway group: {group_name}")


def publish_gateway_message(r, group_name, gateway_domain, message):
    """Publish a message to a hashpipe-redis group gateway <group_name>.
    """
    group_gateway_channel = f"{gateway_domain}:{group_name}///gateway"
    r.publish(group_gateway_channel, message)


def set_group_key(r, array, key, val, l=1, gw_domain="bluse", inst_nums=[0,1]):
    """Publish a message to all instances belonging to an array's associated
    groups (one per instance).
    """
    listeners = 0
    for n in inst_nums:
        group = f"{gw_domain}:{array}-{n}///set"
        listeners += gateway_msg(r, group, key, val, True)
    # check if listeners below expected number:
    if listeners < l:
        missing = l - listeners
        alert(r, f":warning: `{array}` {missing} listeners missing for {key}",
                "coordinator")
    log.info(f"listeners for {key}: {listeners}")
    return listeners

def timestring():
    """A standard format to report the current time in"""
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def pktidx_to_timestamp(r, pktidx, subarray):
    """
    Converts a PKTIDX value into a floating point unix timestamp in UTC, using
    metadata from redis for a given subarray.
    """
    if pktidx < 0:
        raise ValueError(f"cannot convert pktidx {pktidx} to a timestamp")

    # Look in the 0th channel hash for these values
    channel_hash = f"bluse:{subarray}-0///set"

    pipe = r.pipeline()
    for subkey in ["HCLOCKS", "SYNCTIME", "FENCHAN", "CHAN_BW"]:
        pipe.hget(channel_hash, subkey)
    results = pipe.execute()
    hclocks, synctime, fenchan, chan_bw = map(float, results)

    # Seconds since SYNCTIME: PKTIDX*HCLOCKS/(2e6*FENCHAN*ABS(CHAN_BW))
    timestamp = synctime + pktidx * hclocks / (2e6 * fenchan * abs(chan_bw))

    return timestamp


def alert(r, message, name, slack_channel=SLACK_CHANNEL,
          slack_proxy_channel=SLACK_PROXY_CHANNEL):
    """Publish a message to the alerts Slack channel. 
    Args:
        message (str): Message to publish to Slack.
        name (str): Name of process issuing the alert.   
        slack_channel (str): Slack channel to publish message to. 
        slack_proxy_channel (str): Redis channel for the Slack proxy/bridge. 
    Returns:
        None  
    """
    log.info(message)
    # Format: <Slack channel>:<Slack message text>
    alert_msg = f"{slack_channel}:[{timestring()} - {name}] {message}"
    r.publish(slack_proxy_channel, alert_msg)


def retrieve_dwell(r, hpgdomain, host_list, default_dwell):
    """Retrieve the current value of `DWELL` from the Hashpipe-Redis
    Gateway for a specific set of hosts.
    Note that this assumes all instances are host/0.
    Args:
        r (obj): Redis server.
        hpgdomain (str): Hashpipe-Redis gateway domain.
        host_list (str): The list of hosts allocated to the current subarray.
    Returns:
        DWELL (float): The duration for which the processing nodes will record
        for the current subarray (in seconds).
    """
    dwell = default_dwell
    dwell_values = []
    for host in host_list:
        host_key = f"{hpgdomain}://{host}/0/status"
        host_status = r.hgetall(host_key)
        if len(host_status) > 0:
            if 'DWELL' in host_status:
                dwell_values.append(float(host_status['DWELL']))
            else:
                log.warning(f"Cannot retrieve DWELL for {host}")
        else:
            log.warning(f"Cannot access {host}")
    if len(dwell_values) > 0:
        dwell = mode_1d(dwell_values)
        if len(np.unique(dwell_values)) > 1:
            log.warning("DWELL disagreement")
    else:
        log.warning(f"Could not retrieve DWELL. Using {default_dwell} sec by default.")
    return dwell


def mode_1d(data_1d):
    """Calculate the mode of a one-dimensional list.
    Args:
        data_1d (list): List of values for which to calculate the mode.
    Returns:
        mode_1d (float): The most common value in the list.
    """
    vals, freqs = np.unique(data_1d, return_counts=True)
    mode_index = np.argmax(freqs)
    mode_1d = vals[mode_index]
    return mode_1d


def parse_msg(msg):
    """Attempts to parse incoming message from other backend processes.
    Expects a message of the form: <origin>:<message>
    """
    data = msg['data']
    components = data.split(':')
    if len(components) > 4:
        log.warning(f"Unrecognised message: {data}")
        return
    return components


def show_status(r):
    broken = broken_daqs(r)
    if broken:
        print(len(broken), "daqs (bluse_hashpipe) are broken:")
        print(broken)
        print()
    subbed = multicast_subscribed(r)
    print(len(subbed), "hosts are subscribed to F-engine multicast:")
    print(subbed)
    print()
    ready = ready_to_record(r)
    print("the coordinator is ready to record on", len(ready), "hosts:")
    print(ready)
    print()
    recording = get_recording(r)
    if recording:
        print(len(recording), "hosts are currently recording:")
        print(recording)
    else:
        print("no hosts are currently recording")
    dirmap = hosts_by_dir(raw_files(r))
    if not dirmap:
        print()
        print("no hosts have raw files")
    for d, hosts in sorted(dirmap.items()):
        print()
        print(len(hosts), "hosts have raw files in", d, ":")
        print(sorted(hosts))
    for stat, hosts in hpguppi_procstat(r).items():
        if stat in [None, "IDLE", "END"]:
            continue
        print()
        print(len(hosts), "hosts are in hpguppi_proc state", stat, ":")
        print(hosts)

        
def main():
    if len(sys.argv) < 2:
        print("no command specified")
        return
    
    command = sys.argv[1]
    args = sys.argv[2:]
    r = redis.StrictRedis(decode_responses=True)

    if command == "raw_files":
        rawmap = raw_files(r)
        for host, result in sorted(rawmap.items()):
            for r in result:
                print(host, r)
        return

    if command == "sb_id":
        arr = args[0]
        print(sb_id(r, arr))
        return

    if command == "coordinator_subarrays":
        for subarray in coordinator_subarrays(r):
            print(subarray)
        return

    if command == "allocated_hosts":
        subarray = args[0]
        hosts = allocated_hosts(r, subarray)
        print(subarray, "has", len(hosts), "allocated hosts:")
        print(" ".join(hosts))
        return

    if command == "all_hosts":
        print(" ".join(all_hosts(r)))
        return

    if command == "coordinator_hosts":
        hosts = coordinator_hosts(r)
        print("the coordinator is using", len(hosts), "hosts:")
        print(" ".join(hosts))
        return

    if command == "suggest_recording":
        subarrays = suggest_recording(r, verbose=True)
        print(subarrays)
        return

    if command == "suggest_processing":
        dirmap = suggest_processing(r, verbose=True)
        print(sorted(dirmap.keys()))
        return

    if command == "status":
        show_status(r)
        return

    if command == "last_seticore_error":
        host, lines = last_seticore_error(r)
        if host is None:
            print("no recent seticore errors found")
        else:
            print(f"seticore error on {host}:")
            for line in lines:
                print(line)
        return

    if command == "get_bluse_status":
        key = args[0]
        for host, value in sorted(get_status(r, "bluse", key)):
            print(host, value)
        return

    if command == "hpguppi_procstat":
        print()
        for stat, hosts in hpguppi_procstat(r).items():
            print(len(hosts), "hosts are in hpguppi_proc state", stat, ":")
            print(hosts)
            print()
        return

    if command == "pktidx_to_timestamp":
        pktidx_str, subarray = args
        pktidx = int(pktidx_str)
        print(pktidx_to_timestamp(r, pktidx, subarray))
        return
    
    print("unrecognized command:", command)

    
if __name__ == "__main__":
    main()
