"""
Processing module intended to run on each processing node. There should be one
Circus-controlled processing script per instance. They are expected to be
named as follows: proc_<instance number>
"""

import subprocess
import redis
import logging
import sys
import argparse
import os

from automator import proc_util

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
    if not make_outputdir(outputdir, log):
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
        if not make_outputdir(h5dir, log):
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
    parser.add_argument("-n",
                        "--name",
                        type = str,
                        default = "unknown",
                        help = "Name of the current instance.")

    if len(sys.argv[1:]) == 0:
        parser.print_help()
        parser.exit()
    args = parser.parse_args()
    process(name = args.name)

def make_outputdir(outputdir, log):
    """Make an outputdir for seticore search products.
    """
    try:
        os.path.makedirs(outputdir, mode=1777)
        return True
    except FileExistsError:
        log.error("This directory already exists.")
        return False
    except Exception as e:
        log.error(e)
        return False

def process(name):
    """Set up and run processing.
    """

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

    # can we have an arg here for specific directory?
    max_returncode = 0
    for datadir in to_clean:
        res = results[datadir]
        if res > max_returncode:
            max_returncode = res
        if res > 1:
            log.error(f"Not deleting since seticore returned {res} for {datadir}")
            continue
        cmd = ["bash",
                "-c",
                f"/home/obs/bin/cleanmybuf0.sh --force --dir {datadir}"]
        result = subprocess.run(cmd)
        if result == 0:
            log.info(f"Deleted {datadir}")
            continue
        log.error(f"Failed to delete {datadir}, code {result}")

    # Publish result back to central coordinator via Redis:
    r.publish(RESULT_CHANNEL, f"RETURN:{name}:{max_returncode}")

if __name__ == "__main__":
    cli()