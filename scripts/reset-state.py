"""Reset coordinator state machines to specified states. 
"""

import argparse
import sys
import redis

from coordinator import redis_util

def cli(args = sys.argv[0]):
    usage = "{} [options]".format(args)
    description = "Analyse prior observations by crawling recordings."
    parser = argparse.ArgumentParser(prog = "obs-stat",
                                     usage = usage,
                                     description = description)
    parser.add_argument("--all",
                        action = "store_true",
                        default = False,
                        help = 'Reset all to `ready`, `unsubscribed`')
    parser.add_argument("--rec",
                        type = str,
                        default = "array_1",
                        help = 'Reset all in subarray to `ready` from `rec`')

    if len(sys.argv[1:]) == 0:
        parser.print_help()
        parser.exit()
    args = parser.parse_args()

    if args.all:
        Res = Reset()
        Res.reset_all()

    if args.rec:
        Res = Reset()
        Res.reset_rec(args.rec)

class Reset:

    def __init__(self):
        self.r = redis.StrictRedis(decode_responses=True)
        self.subarrays = ["array_1", "array_2", "array_3", "array_4"]

    def reset_all(self):
        """Reset to `ready`, `unsubscribed` for all subarrays.
        """
        self.r.delete("free_instances")
        keys = 0
        for subarray in self.subarrays:
            keys += self.r.delete(f"{subarray}:state")
            keys += self.r.delete(f"{subarray}:freesub_state")
        print(f"{keys} keys cleared")

    def reset_rec(self, subarray):
        """Recording reset, resets subarray state to `READY` and moves all
        recording instances back to the `ready` set from `recording`.
        """
        state = redis_util.read_state(subarray, self.r)
        if state:
            while state["recording"]:
                state["ready"].add(state["recording"].pop())
            state["recproc_state"] = "READY"
            redis_util.save_state(subarray, state, self.r)

if __name__ == "__main__":
    cli()