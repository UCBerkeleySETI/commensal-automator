import subprocess
from automator import redis_util

from .logger import log

def run_seticore(hosts, bfrdir, inputdir, timestamped_dir, partition, r):
    """Processes the incoming data using seticore.

    Args:
        hosts (List[str]): List of processing node host names assoctated
        with the current subarray.
        bfrdir (str): Directory containing the beamformer recipe files 
        associated with the data in the NVMe modules. 
        arrayid (str): Name of the current subarray. 
        inputdir (str): Directory containing raw file input
        timestamped_dir (str): directory component starting with a timestamp
        partition (str): partition component of output directory.
        r (obj): redis server.

    Returns:
        None
    """

    # Create output directories:
    outputdir = f"/{partition}/data/{timestamped_dir}/seticore_search"
    log.info("Creating output directories...") 
    log.info(f"Search: {outputdir}")
    for host in hosts:
        cmd = ["ssh", host, "mkdir", "-p", "-m", "1777", outputdir]
        subprocess.run(cmd)

    # Build slurm command:
    seticore_command = ["/home/lacker/bin/seticore",
                        "--input", inputdir,
                        "--output", outputdir, 
                        "--snr", "6",
                        "--num_bands", "16",
                        "--fine_channels", "8388608",
                        "--telescope_id", "64",
                        "--recipe_dir", bfrdir]

    # Check number of times a processing sequence has been run and write .h5
    # files for each beamformer output
    n = redis_util.get_n_proc(r)
    if n%10 == 0:
        # create directory for h5 files
        h5dir = f"/{partition}/data/{timestamped_dir}/seticore_beamformer"
        log.info("Creating beamformer output directory...")
        log.info(f"Beamformer: {h5dir}")
        for host in hosts:
            cmd = ["ssh", host, "mkdir", "-p", "-m", "1777", h5dir]
            subprocess.run(cmd)
        # add --h5_dir arg to seticore command
        seticore_command.extend(["--h5_dir", h5dir])
    redis_util.increment_n_proc(r)

    err = "/home/obs/seticore_slurm/seticore_%N.err"
    out = "/home/obs/seticore_slurm/seticore_%N.out"
    cmd = (["srun", "--open-mode=append", "-e", err, "-o", out, "-w"] +
           [" ".join(hosts)] + seticore_command)
    log.info(f"running seticore: {cmd}")
    result = subprocess.run(cmd).returncode
    return result
