import redis
import time
import os
import subprocess

from .logger import log

def run_seticore(hosts, bfrdir, inputdir, sb_id):
    """Processes the incoming data using seticore.

    Args:
        hosts (List[str]): List of processing node host names assoctated
        with the current subarray.
        bfrdir (str): Directory containing the beamformer recipe files 
        associated with the data in the NVMe modules. 
        arrayid (str): Name of the current subarray. 
        inputdir (str): Directory containing raw file input
        sb_id (str): the schedule block id

    Returns:
        None
    """
    # Create output directories:
    outputdir = '/scratch/data/{}/seticore_search'.format(sb_id)
    h5dir = '/scratch/data/{}/seticore_beamformer'.format(sb_id)
    log.info('Creating output directories...') 
    log.info('\nsearch: {}\nbeamformer: {}'.format(outputdir, h5dir))
    for host in hosts:
        cmd = ['ssh', host, 'mkdir', '-p', '-m', '1777', outputdir]
        subprocess.run(cmd)
        cmd = ['ssh', host, 'mkdir', '-p', '-m', '1777', h5dir]
        subprocess.run(cmd)

    # Build slurm command:
    seticore_command = ['/home/lacker/bin/seticore',
                        '--input', inputdir,
                        '--output', outputdir, 
                        '--snr', '6',
                        '--h5_dir', h5dir, 
                        '--num_bands', '16',
                        '--fine_channels', '8388608',
                        '--telescope_id', '64',
                        '--recipe_dir', bfrdir]
    err = '/home/obs/seticore_slurm/seticore_%N.err'
    out = '/home/obs/seticore_slurm/seticore_%N.out'
    cmd = (['srun', '--open-mode=append', '-e', err, '-o', out, '-w'] +
           [' '.join(hosts)] + seticore_command)
    log.info('running seticore: {}'.format(cmd))
    result = subprocess.run(cmd).returncode
    return result
