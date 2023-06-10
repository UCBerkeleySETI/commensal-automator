import subprocess
import redis
import logging
import sys
import argparse

RESULT_CHANNEL = "proc_result"
LOG_FORMAT = "[%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s] %(message)s"
LOGGER_NAME = "BLUSE.interface"

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
    cmd = ["mkdir", "-p", "-m", "1777", outputdir]
    subprocess.run(cmd)

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
    n = get_n_proc(r)
    if n%10 == 0:
        # create directory for h5 files
        h5dir = f"/{partition}/data/{tsdir}/seticore_beamformer"
        log.info(f"Creating beamformer output directory: {h5dir}")
        cmd = ["mkdir", "-p", "-m", "1777", h5dir]
        subprocess.run(cmd)
        # add --h5_dir arg to seticore command
        seticore_command.extend(["--h5_dir", h5dir])
    increment_n_proc(r)

    # run seticore
    log.info(f"running seticore: {seticore_command}")
    return subprocess.run(seticore_command).returncode

def increment_n_proc(r):
    """Add 1 to the number of times processing has been run.
    """
    n_proc = get_n_proc(r)
    r.set("automator:n_proc", n_proc + 1)

def get_n_proc(r):
    """Retrieve the absolute number of times processing has been run.
    """
    n_proc = r.get("automator:n_proc")
    if n_proc is None:
        r.set("automator:n_proc", 0)
        return 0
    return int(n_proc)

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

def process(name):
    """Set up and run processing.
    """

    # Set up logging:
    log = logging.getLogger(LOGGER_NAME)
    logging.basicConfig(format=LOG_FORMAT)
    log.setLevel(level=logging.DEBUG)

    # Redis server
    r = redis.StrictRedis(decode_responses=True)

    # Look for hash for specific node
    params = r.hgetall(f"{name}:proc")

    # Run seticore
    result = run_seticore(
        params["bfrdir"],
        params["inputdir"],
        params["tsdir"],
        params["partition"],
        r, 
        log)

    # Publish result back to central coordinator via Redis:
    r.publish(RESULT_CHANNEL, f"{name}:{result}")

    # Done
    log.info(f"Processing completed for {name} with code: {result}")

if __name__ == "__main__":
    cli()