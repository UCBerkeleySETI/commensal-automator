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


def active_subarrays(r):
    """Returns a list of all subarrays that have hosts allocated to them."""
    return sorted(key.split(":")[-1] for key in r.keys("coordinator:allocated_hosts:*"))


def allocated_hosts(r, subarray):
    """Returns the hosts allocated to a particular subarray."""
    results = r.lrange("coordinator:allocated_hosts:" + subarray, 0, -1)
    return sorted(s.split("/")[0] for s in results)


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

    if command == "active_subarrays":
        for subarray in active_subarrays(r):
            print(subarray)
        return

    if command == "allocated_hosts":
        subarray = args[0]
        hosts = allocated_hosts(r, subarray)
        print(subarray, "has", len(hosts), "allocated hosts:")
        print(" ".join(hosts))
        return
    
    print("unrecognized command:", command)

if __name__ == "__main__":
    main()
