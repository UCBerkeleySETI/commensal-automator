#!/usr/bin/env python
# Helpers functions for looking up various redis data.
# The convention is that "r" is our redis client.

import redis
import sys

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
        pktidx, pktstart, pktstop = map(int, strkeys)
        if pktstart > 0 and pktidx < pktstop:
            answer.add(host)
    return sorted(answer)
    

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
    
    print("unrecognized command:", command)

if __name__ == "__main__":
    main()
