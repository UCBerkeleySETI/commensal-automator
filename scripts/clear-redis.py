"""
Script to clear calibration solutions from Redis older than a specific
number of days. 
"""

import sys
import argparse
import time
import redis

def clear(array, days):
    """Remove all cal solutions (and their ZSET entries) older than
    the specified number of days.
    """
    r = redis.StrictRedis(decode_responses=True)
    index = time.time() - days*86400
    zkey = f"{array}:cal_solutions:index"
    # all keys from before specified index. 
    entries = r.zrangebyscore(zkey, 0, index)
    print(f"Removing {len(entries)} entries")
    if len(entries) > 0:
        # remove keys/contents:
        r.delete(*entries)
        # remove the keys from the zset:
        r.zrem(zkey, *entries)

def cli(args = sys.argv[0]):
    """CLI for script to clear calibration solutions from Redis. 
    """
    usage = f"{args} [options]"
    description = "Remove all cal solutions older than specified number of days."
    parser = argparse.ArgumentParser(usage = usage,
                                     description = description)
    parser.add_argument("-a",
                        "--array",
                        type = str,
                        default = "array_1",
                        help = "Subarray name.")
    parser.add_argument("-d",
                        "--days",
                        type = int,
                        default = 90,
                        help = "Delete cal solutions older than this in days.")
    if len(sys.argv[1:]) == 0:
        parser.print_help()
        parser.exit()
    args = parser.parse_args()
    clear(array = args.array, days = args.days)

if __name__ == "__main__":
    cli()