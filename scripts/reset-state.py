"""Reset coordinator state machines to specified states. 
"""

import argparse
import sys
import redis

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

    if len(sys.argv[1:]) == 0:
        parser.print_help()
        parser.exit()
    args = parser.parse_args()

    if args.all:
        Res = Reset()
        Res.reset_all()

class Reset:

    def __init__(self):
        self.r = redis.StrictRedis(decode_responses=True)
        self.subarrays = ["array_1", "array_2", "array_3", "array_4"]

    def reset_all(self):
        """Reset to `ready`, `unsubscribed` for all subarrays.
        """
        self.r.delete("free_instances")
        for subarray in self.subarrays:
            self.r.delete(f"{subarray}:state")
            self.r.delete(f"{subarray}:freesub_state")

if __name__ == "__main__":
    cli()