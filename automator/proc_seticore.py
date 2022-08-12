import redis
import time
import os
import subprocess

from .logger import log

class ProcSeticore(object):
    """This class controls data processing using seticore [1], which performs 
    upchannelisation, beamforming and narrowband doppler drift searching 
    all-in-one.

    It is designed to be imported as a processing module by the automator.

    [1] https://github.com/lacker/seticore
    """   

    def __init__(self):
        self.redis_server = redis.StrictRedis(decode_responses=True)
        self.PROC_STATUS_KEY = 'PROCSTAT'

    def process(self, seticore, hosts, bfrdir, arrayid):
        """Processes the incoming data using seticore.

        Args:
            seticore (str): Location of current version of seticore. 
            hosts (List[str]): List of processing node host names assoctated
            with the current subarray.
            bfrdir (str): Directory containing the beamformer recipe files 
            associated with the data in the NVMe modules. 
            arrayid (str): Name of the current subarray. 

        Returns:
            None
        """
        # Determine input directory:
        rawfiles = self.redis_server.smembers('bluse_raw_watch:{}'.format(hosts[1]))
        inputdirs = [os.path.dirname(rawfile) for rawfile in rawfiles]
        if(len(inputdirs) > 0):
            # Get mode and determine FFT size:
            fenchan = self.redis_server.get('{}:n_channels'.format(arrayid))
            # Change FFT size to achieve roughly 1Hz channel bandwidth
            if(fenchan == '32768'):
                fft_size = str(2**14)
            elif(fenchan == '4096'):
                fft_size = str(2**17)
            elif(fenchan == '1024'):
                fft_size = str(2**19)
            else:
                log.error('Unexpected FENCHAN: {}'.format(fenchan))
                fft_size = str(2**14)
            # SB ID:
            datadir = self.redis_server.get('{}:current_sb_id'.format(arrayid))
            # Create output directories:
            log.info('Creating output directories...')
            outputdir = '/scratch/data/{}/seticore_search'.format(datadir)
            h5dir = '/scratch/data/{}/seticore_beamformer'.format(datadir)
            for host in hosts:
                cmd = ['ssh', host, 'mkdir', '-p', '-m', '1777', outputdir]
                subprocess.run(cmd)
                cmd = ['ssh', host, 'mkdir', '-p', '-m', '1777', h5dir]
                subprocess.run(cmd)
            # Build slurm command:
            seticore_args = ['--input', inputdirs[0], 
                             '--output', outputdir, 
                             '--h5_dir', h5dir, 
                             '--num_bands', '16',
                             '--fft_size', fft_size,
                             '--telescope_id', '64',
                             '--recipe_dir', bfrdir]
            cmd = ['srun', '-w'] + [' '.join(hosts)] + [seticore] + seticore_args
            log.info('Running seticore: {}'.format(cmd))
            subprocess.run(cmd)
            return True
        else:
            log.info('No data to process')
            return False
            