"""
Command line interface for the coordinator, to be run as a singleton on
the headnode.
"""

import argparse
import sys

from coordinator import Coordinator
from logger import log, set_logger


def cli(args = sys.argv[0]):
    usage = "{} [options]".format(args)
    description = "Start the coordinator"
    parser = argparse.ArgumentParser(prog = "coordinator",
                                     usage = usage,
                                     description = description)
    parser.add_argument("--config",
                        type = str,
                        default = 'config.yml',
                        help = 'Config file location.')

    if len(sys.argv[1:]) == 0:
        parser.print_help()
        parser.exit()
    args = parser.parse_args()
    start(config_file = args.redis_endpoint)


def start(config_file):
    """Start the coordinator.
    """
    set_logger("DEBUG")
    BluseCoordinator = Coordinator(config_file)
    BluseCoordinator.start()


if __name__ == "__main__":
    cli()