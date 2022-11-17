import redis
import time
import os
import subprocess

from .logger import log

def run_seticore(hosts, bfrdir, inputdir, timestamp):
    """Processes the incoming data using seticore.

    Args:
        hosts (List[str]): List of processing node host names assoctated
        with the current subarray.
        bfrdir (str): Directory containing the beamformer recipe files 
        associated with the data in the NVMe modules. 
        arrayid (str): Name of the current subarray. 
        inputdir (str): Directory containing raw file input
        timestamp (str): timestamp of recording start, in <day>T<time>Z format

    Returns:
        None
    """
    # Create output directories:
    outputdir = f"/scratch/data/{timestamp}/seticore_search"
    h5dir = f"/scratch/data/{timestamp}/seticore_beamformer"
    log.info("Creating output directories...") 
    log.info(f"\nsearch: {outputdir}\nbeamformer: {h5dir}")
    for host in hosts:
        cmd = ["ssh", host, "mkdir", "-p", "-m", "1777", outputdir]
        subprocess.run(cmd)
        cmd = ["ssh", host, "mkdir", "-p", "-m", "1777", h5dir]
        subprocess.run(cmd)

    # Build slurm command:
    seticore_command = ["/home/lacker/bin/seticore",
                        "--input", inputdir,
                        "--output", outputdir, 
                        "--snr", "6",
                        "--h5_dir", h5dir, 
                        "--num_bands", "16",
                        "--fine_channels", "8388608",
                        "--telescope_id", "64",
                        "--recipe_dir", bfrdir]
    err = "/home/obs/seticore_slurm/seticore_%N.err"
    out = "/home/obs/seticore_slurm/seticore_%N.out"
    cmd = (["srun", "--open-mode=append", "-e", err, "-o", out, "-w"] +
           [" ".join(hosts)] + seticore_command)
    log.info(f"running seticore: {cmd}")
    result = subprocess.run(cmd).returncode
    return result
