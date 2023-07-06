"""
Processing module intended to run on each processing node. There should be one
Circus-controlled processing script per instance. 
"""

import subprocess
import redis
import logging
import sys
import argparse
import os
import socket

import proc_util

RESULT_CHANNEL = "proc_result"
LOG_FORMAT = "[%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s] %(message)s"
LOGGER_NAME = "BLUSE.interface"
BFRDIR = "/home/obs/bfr5"
PARTITION = "scratch"

def run_seticore(bfrdir, inputdir, tsdir, partition, r, log):
    """Processes the incoming data using seticore.

    Args:
        bfrdir (str): Directory containing the beamformer recipe files 
        associated with the data in the NVMe modules. 
        inputdir (str): Directory containing raw file input
        tsdir (str): directory component starting with a timestamp
        partition (str): partition component of output directory.
        r (obj): redis server.

    Returns:
        None
    """
    # Create search product output directory.
    outputdir = f"/{partition}/data/{tsdir}/seticore_search"
    log.info(f"Creating search output directory: {outputdir}")
    if not proc_util.make_outputdir(outputdir, log):
        return 2

    # Build command:
    seticore_command = ["/home/lacker/bin/seticore",
                        "--input", inputdir,
                        "--output", outputdir,
                        "--snr", "6",
                        "--num_bands", "16",
                        "--fine_channels", "8388608",
                        "--telescope_id", "64",
                        "--recipe_dir", bfrdir]

    # Check number of times a processing sequence has been run and write .h5
    # files for each beamformer output for every tenth run.
    n = proc_util.get_n_proc(r)
    if n%10 == 0:
        # create directory for h5 files
        h5dir = f"/{partition}/data/{tsdir}/seticore_beamformer"
        log.info(f"Creating beamformer output directory: {h5dir}")
        if not proc_util.make_outputdir(h5dir, log):
            return 2
        # add --h5_dir arg to seticore command
        seticore_command.extend(["--h5_dir", h5dir])
    proc_util.increment_n_proc(r)

    # run seticore
    log.info(f"running seticore: {seticore_command}")
    return subprocess.run(seticore_command).returncode

def cli(args = sys.argv[0]):
    """CLI for instance-specific processing controller. 
    """
    usage = f"{args} [options]"
    description = "Add or remove sources from targets database."
    parser = argparse.ArgumentParser(usage = usage,
                                     description = description)
    parser.add_argument("-I",
                        "--instance",
                        type = int,
                        default = 0,
                        help = "Current instance number.")
    if len(sys.argv[1:]) == 0:
        parser.print_help()
        parser.exit()
    args = parser.parse_args()
    process(n = args.instance)


def process(n):
    """Set up and run processing.
    """
    # Get the hostname
    host = socket.gethostname().split(".")[0]

    name = "f{host}/{n}"

    # Set up logging:
    log = logging.getLogger(LOGGER_NAME)
    logging.basicConfig(format=LOG_FORMAT)
    log.setLevel(level=logging.DEBUG)

    # Redis server
    r = redis.StrictRedis(decode_responses=True)

    # Set of unprocessed directories:
    unprocessed = proc_util.get_items(r, name, "unprocessed")

    # Set of directories that should be kept after processing (these are
    # directories associated with a primary observation)
    preserved = proc_util.get_items(r, name, "preserved")

    results = dict()

    for datadir in unprocessed:
        if not os.path.exists(datadir):
            log.warning(f"{datadir} does not exist, skipping.")
            continue
        # Timestamped directory name:
        tsdir = proc_util.timestamped_dir_from_filename(datadir)
        # Run seticore
        result = run_seticore(
            BFRDIR,
            datadir,
            tsdir,
            PARTITION,
            r,
            log)
        results[datadir] = result

    # Done
    log.info(f"Processing completed for {name} with code: {result}")

    # Clean up
    to_clean = unprocessed.difference(preserved)

    max_returncode = 0
    for datadir in to_clean:
        res = results[datadir]
        if res > 1:
            log.error(f"Not deleting since seticore returned {res} for {datadir}")
            continue
        if not os.exists(datadir):
            log.warning("Directory doesn't exist")
            continue
        if not proc_util.rm_datadir(datadir, n, log):
            log.error(f"Failed to clear {datadir}")
            res = 2
        if res > max_returncode:
            max_returncode = res

    # Publish result back to central coordinator via Redis:
    r.publish(RESULT_CHANNEL, f"RETURN:{name}:{max_returncode}")


if __name__ == "__main__":
    cli()