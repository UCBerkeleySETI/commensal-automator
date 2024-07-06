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
import sys
import time

from coordinator import proc_util
from coordinator import redis_util

RESULT_CHANNEL = "proc_result"
PRIORITY_CHANNEL = "target-selector:processing"
LOG_FORMAT = "[%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s] %(message)s"
LOGGER_NAME = "BLUSE.interface"
BFRDIR = "/home/obs/bfr5"
REDIS_HOST = "10.98.81.254"

def run_seticore(bfrdir, inputdir, tsdir, volume, r, log):
    """Processes the incoming data using seticore.

    Args:
        bfrdir (str): Directory containing the beamformer recipe files 
        associated with the data in the NVMe modules. 
        inputdir (str): Directory containing raw file input
        tsdir (str): directory component starting with a timestamp
        volume (str): volume component of output directory.
        r (obj): redis server.

    Returns:
        None
    """
    # Create raw file input directory. Note we assume GUPPI RAW path with
    # "Unknown/GUPPI" subdirectories containing the actual raw files. 
    inputdir = f"{inputdir}/Unknown/GUPPI"
    # Create search product output directory.
    outputdir = f"/{volume}/data/{tsdir}/seticore_search"
    log.info(f"Creating search output directory: {outputdir}")
    if not proc_util.make_outputdir(outputdir, log):
        return 1

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
        h5dir = f"/{volume}/data/{tsdir}/seticore_beamformer"
        log.info(f"Creating beamformer output directory: {h5dir}")
        if not proc_util.make_outputdir(h5dir, log):
            return 1
        # add --h5_dir arg to seticore command
        seticore_command.extend(["--h5_dir", h5dir])

    # run seticore
    return subprocess.run(seticore_command).returncode

def cli(args = sys.argv[0]):
    """CLI for instance-specific processing controller. 
    """
    usage = f"{args} [options]"
    description = "Run processing actions for specified instance."
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

def ml_detection(tsdir, outputvolume, log):
    """Run detection code.
    """
    # ts = tsdir.split("/")[2].split("-")[0] # ToDo: get rid of
    ts = tsdir.split("-")[0] # ToDo: get rid of
    array = "array_1" # ToDo: autodetect
    bfr5file = f"/home/obs/bfr5/MeerKAT-{array}-{ts}.bfr5"

    # inputdir
    inputdir = f"/{outputvolume}/data/{tsdir}/seticore_beamformer"
    # outputdir
    outputdir = f"/{outputvolume}/data/{tsdir}/ml_search"
    log.info(f"Creating ml output directory: {outputdir}")
    if not proc_util.make_outputdir(outputdir, log):
        return 1

    cmd = ["detection",
        "-b", inputdir,
        "-r", bfr5file,
        "-o", outputdir,
        "-s", "1000",
        "-m", "0.7"]

    log.info(cmd)

    return subprocess.run(cmd).returncode

def process(n):
    """Set up and run processing.
    """

    # Set up logging:
    log = logging.getLogger(LOGGER_NAME)
    logging.basicConfig(format=LOG_FORMAT)
    log.setLevel(level=logging.DEBUG)

    # Get the hostname
    host = socket.gethostname().split(".")[0]
    name = f"{host}/{n}"
    log.info(f"Launching analyzer for {name}")

    # Redis server
    r = redis.StrictRedis(host=REDIS_HOST, decode_responses=True)

    # Set of unprocessed directories:
    unprocessed = proc_util.get_items(r, name, "unprocessed")
    log.info(f"{len(unprocessed)} unprocessed directory(ies)")

    # Check against current recording's DATADIR
    to_clean = set()
    last_datadir = r.get(f"{name}:last-datadir")
    if last_datadir not in unprocessed:
        log.warning(f"DATADIR mismatch, last datadir was {last_datadir}, current datadirs are {unprocessed}")
        redis_util.alert(r, f":warning: `DATADIR` mismatch",
        f"analyzer:{name}")
        # add to list for deletion
        to_clean.add(last_datadir)

    # Volume
    volume = "scratch"
    if n == 1:
        volume = "scratch2"
    elif n != 0:
        log.warning(f"Instance no. {n}, defaulting to /scratch")

    max_returncode = 0
    max_ml_returncode = -1

    if unprocessed:
        # Set of directories that should be kept after processing (these are
        # directories associated with a primary observation)
        preserved = proc_util.get_items(r, name, "preserved")

        results = dict()
        results_ml = dict()

        for datadir in unprocessed:

            if not os.path.exists(datadir):
                log.warning(f"{datadir} does not exist, skipping.")
                max_returncode = max(max_returncode, 1)
                continue
            if not os.listdir(datadir):
                log.warning(f"{datadir} empty, skipping.")
                max_returncode = max(max_returncode, 1)
                continue

            # If recording is shorter than 2.5 minutes, ignore
            if not proc_util.check_length(r, datadir, 150):
                results[datadir] = 0
                log.info("Too short, skipping")
                continue

            # Timestamped directory name:
            tsdir = proc_util.timestamped_dir_from_filename(datadir)
            # Run seticore
            result = run_seticore(
                BFRDIR,
                datadir,
                tsdir,
                volume,
                r,
                log)
            results[datadir] = result

            # Each datadir corresponds with a unique obsid. Instruct target
            # selector to update the priorty table for each completed datadir.
            if result == 0:
                proc_util.completed(r, datadir, 1, 64, PRIORITY_CHANNEL)

            # run ML detection script:
            if proc_util.get_n_proc(r)%10 == 0:
                try:
                    ml_code = ml_detection(tsdir, volume, log)
                except Exception as e:
                    log.info("ML detection error:")
                    log.error(e)
                    ml_code = 1
                results_ml[datadir] = ml_code
                max_ml_returncode = max(max_ml_returncode, ml_code)

            # overall max_returncode for this analyser execution:
            max_returncode = max(max_returncode, result)

        # Done
        log.info(f"Processing completed for {name} with codes: {results} and {results_ml}")

        # Clean up
        to_clean = to_clean.union(unprocessed.difference(preserved))

        for datadir in to_clean:
            if datadir not in results:
                log.error(f"Trying to clear {datadir}, but it has no returncodes.")
                max_returncode = max(max_returncode, 1)
                continue
            res = results[datadir]
            if res > 1:
                log.error(f"Not deleting since seticore returned {res} for {datadir}")
                continue
            if not os.path.exists(datadir):
                log.warning("Directory doesn't exist")
                max_returncode = max(max_returncode, 1)
                continue
            log.info(f"Deleting: {datadir}")
            for i in range(0,3):
                if not proc_util.rm_datadir(datadir, n, log):
                    log.error(f"Failed to clear {datadir}")
                    max_returncode = max(max_returncode, 2)
                # check if actually deleted
                if os.path.isdir(datadir):
                    log.error("Unsuccessful deletion, attempting again")
                    redis_util.alert(r, f":warning: deletion failure, retry",
                    f"analyzer:{name}")
                else:
                    break


    # Publish result back to central coordinator via Redis:
    r.publish(RESULT_CHANNEL, f"RETURN:{name}:{max_returncode}:{max_ml_returncode}")

    if max_returncode > 1:
        redis_util.alert(r, f":warning: `{name}` code: `{max_returncode}`",
                "analyzer")


if __name__ == "__main__":
    cli()
