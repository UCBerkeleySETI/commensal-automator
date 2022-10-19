#!/usr/bin/env python
# Helpers functions for looking up various redis data.
# The convention is that "r" is our redis client.

import redis
import sys

def raw_files(r):
    """Returns a dict mapping host name to a list of raw files on the host."""
    hosts = r.keys("bluse_raw_watch:*")
    pipe = r.pipeline()
    for host in hosts:
        pipe.set("smembers", "bluse_raw_watch:" + host)
    results = pipe.execute()
    answer = {}
    for host, result in zip(hosts, results):
        answer[host] = sorted(result)
    return answer


def sb_id(r, subarray):
    return r.get('{}:current_sb_id'.format(subarray))


def main():
    if len(sys.argv) < 2:
        print("no command specified")
        return
    
    command = sys.argv[1]
    args = sys.argv[2:]
    r = redis.StrictRedis(decode_responses=True)

    if command == "raw_files":
        hosts = ["blpn{}".format(i) for i in range(64)]
        for host, result in raw_files(r):
            for r in result:
                print(host, r)
        return

    if command == "sb_id":
        arr = args[0]
        print(sb_id(r, arr))
        return
    
    print("unrecognized command:", command)

if __name__ == "__main__":
    main()
