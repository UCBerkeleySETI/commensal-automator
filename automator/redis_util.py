#!/usr/bin/env python
# Helpers functions for looking up various redis data.
# The convention is that "r" is our redis client.

import os
import redis
import sys

from .logger import log

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
    return r.get('{}:current_sb_id'.format(subarray))


def coordinator_subarrays(r):
    """Returns a list of all subarrays that have hosts allocated to the coordinator."""
    return sorted(key.split(":")[-1] for key in r.keys("coordinator:allocated_hosts:*"))


def allocated_hosts(r, subarray):
    """Returns the hosts allocated to a particular subarray."""
    results = r.lrange("coordinator:allocated_hosts:" + subarray, 0, -1)
    return sorted(s.split("/")[0] for s in results)


def get_nshot(r, subarray_name):
    """Get the current value of nshot from redis.
    """
    nshot_key = 'coordinator:trigger_mode:{}'.format(subarray_name)
    nshot = int(r.get(nshot_key).split(':')[1])
    return nshot


def all_hosts(r):
    return sorted(key.split("//")[-1].split("/")[0]
                  for key in r.keys("bluse://*/0/status"))


def coordinator_hosts(r):
    """Returns a list of all hosts the coordinator is using.

    These are the hosts that the coordinator may write more files to, even if it
    receives no further instructions from us.
    We don't want to start processing on a host while the coordinator is still using it.

    TODO: carefully avoid all race conditions here
    """
    # First find all the hosts that have nshot=1
    answer = set()
    subarrays = coordinator_subarrays(r)
    for subarray in subarrays:
        nshot = get_nshot(r, subarray)
        if nshot > 0:
            answer = answer.union(allocated_hosts(r, subarray))

    pipe = r.pipeline()
    hosts = all_hosts(r)
    for host in hosts:
        key = "bluse://{}/0/status".format(host)
        pipe.hmget(key, ["PKTIDX", "PKTSTART", "PKTSTOP"])
    results = pipe.execute()
    for host, strkeys in zip(hosts, results):
        if None in strkeys:
            # This seems to happen sometimes during recording.
            # Let's treat it as "in use"
            answer.add(host)
            continue
        pktidx, pktstart, pktstop = map(int, strkeys)
        if pktstart > 0 and pktidx < pktstop:
            answer.add(host)
    return sorted(answer)


def sb_id_from_filename(filename):
    """Works on either raw file names or directory names.
    Returns None if the filename doesn't fit the pattern.
    """
    parts = filename.strip("/").split("/")
    if len(parts) < 2:
        return None
    x, y = parts[:2]
    if len(x) != 8 or not x.isnumeric() or not y.isnumeric():
        return None
    return "{}/{}".format(x, y)


def infer_subarray(r, hosts):
    """Guess what subarray the data on these hosts is from.
    If there is no exact match, return None.
    """
    hosts_set = set(hosts)
    for subarray in coordinator_subarrays(r):
        array_hosts = allocated_hosts(r, subarray)
        if hosts_set == set(array_hosts):
            return subarray
    return None


def suggest_recording(r, processing=None, verbose=False):
    """Returns a list of all subarrays that we can start recording on.

    processing is a set of hosts that are busy processing, so we can't
    use them to record.
    """
    # Determine what hosts are already being used
    busy = set()
    if processing is not None:
        busy = busy.union(processing)
        if verbose:
            print("hosts that are busy processing:", sorted(busy))
    elif verbose:
        print("assuming there is no processing happening right now")
    hosts_with_files = set(raw_files(r).keys())
    if verbose:
        print("hosts that have raw files:", sorted(hosts_with_files))
    busy = busy.union(hosts_with_files)
    coord_using = coordinator_hosts(r)
    if verbose:
        print("hosts that the coordinator is already using:", sorted(coord_using))
    busy = busy.union(coord_using)

    # See what subarrays don't want any of these hosts
    subarrays = coordinator_subarrays(r)
    if not subarrays:
        if verbose:
            print("no subarrays are active, so we can't record")
        return []
    
    answer = []
    for subarray in subarrays:
        hosts = set(allocated_hosts(r, subarray))
        inter = busy.intersection(hosts)
        if inter:
            if verbose:
                print("we cannot record on {} because {} are in use".format(
                    subarray, sorted(inter)))
            continue
        if verbose:
            print("we can record on {} because {} are unused".format(
                subarray, sorted(hosts)))
        answer.append(subarray)
    return answer


def suggest_processing(r, processing=None, verbose=False):
    """Returns a map of (input dir, set of hosts) tuples that we could process.

    processing is a set of hosts that are already busy processing, so we can't
    use them to start a new round of processing.
    """
    # Determine what hosts we can use
    busy = set()
    if processing is not None:
        busy = busy.union(processing)
        if verbose:
            print("hosts that are already processing:", sorted(busy))
    elif verbose:
        print("assuming there is no processing already happening")
    coord_using = coordinator_hosts(r)
    if verbose:
        print("hosts that the coordinator is using:", sorted(coord_using))
    busy = busy.union(coord_using)

    # See what files there are to process
    filemap = raw_files(r)
    if not filemap:
        if verbose:
            print("there are no raw files anywhere, so we can't process")
        return {}

    # Create a map of input directory to a set of hosts, without checking busy
    potential = {}
    for host, filenames in filemap.items():
        for filename in filenames:
            dirname = os.path.dirname(filename)
            if dirname not in potential:
                potential[dirname] = set()
            potential[dirname].add(host)

    # Filter for input directories where none of the hosts are busy
    answer = {}
    for dirname, hosts in potential.items():
        inter = hosts.intersection(busy)
        if inter:
            if verbose:
                print("{} has busy hosts: {}".format(dirname, sorted(inter)))
        else:
            if verbose:
                print("{} is ready on hosts: {}".format(dirname, sorted(hosts)))
            answer[dirname] = hosts

    return answer
            
def join_gateway_group(r, hosts, group_name, gateway_domain):
    """Instruct hashpipe instances to join a hashpipe-redis gateway group.
    
    Hashpipe-redis gateway keys can be published for all these nodes
    simultaneously by publishing to the Redis channel:
    <gateway_domain>:<group_name>///set
    """
    # Instruct each host to join specified group:
    for i in range(len(allocated_hosts)):
        node_gateway_channel = '{}://{}/gateway'.format(gateway_domain, hosts[i])
        msg = 'join={}'.format(group_name)
        r.publish(node_gateway_channel, msg)
    log.info('Hosts {} instructed to join gateway group: {}'.format(hosts, group_name))

def leave_gateway_group(r, group_name, gateway_domain):
    """Instruct hashpipe instances to leave a hashpipe-redis gateway group.
    """
    message = 'leave={}'.format(group_name)
    publish_group_message(r, group_name, gateway_domain, message)
    log.info('Hosts instructed to leave the gateway group: {}'.format(group_name))

def publish_group_message(r, group_name, gateway_domain, message):
    """Publish a message to a hashpipe-redis gateway group <group_name>.
    """
    group_gateway_channel = '{}:{}///gateway'.format(gateway_domain, group_name)
    r.publish(group_gateway_channel, message)
    log.info('Message: {} published to gateway group: {}'.format(message, group_name))

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

    if command == "get_nshot":
        subarray = args[0]
        nshot = get_nshot(r, subarray)
        print(nshot)
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
    
    print("unrecognized command:", command)

    
if __name__ == "__main__":
    main()
