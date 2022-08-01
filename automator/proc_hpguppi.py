import redis
import time
import os
import subprocess

from .logger import log


class ProcHpguppi(object):

    def __init__(self):
        
        self.redis_server = redis.StrictRedis(decode_responses=True)
        self.PROC_STATUS_KEY = 'PROCSTAT'

    def process(self, proc_domain, hosts, subarray, bfrdir, outdir):
        """Process the incoming data via hpguppi proc

        """
        # Find list of files:
        datadir = self.redis_server.get('{}:current_sb_id'.format(subarray))
        rawfiles = self.redis_server.smembers('bluse_raw_watch:{}'.format(hosts[0])) 
        # Temporarily remove full file path:
        rawfiles_only = [os.path.basename(rawfile) for rawfile in rawfiles]
        inputdirs = [os.path.dirname(rawfile) for rawfile in rawfiles] 
        inputdir = inputdirs[0]

        # Set keys to prepare for processing:
        group_chan = '{}:{}///set'.format(proc_domain, subarray)
        self.redis_server.publish(group_chan, 'BFRDIR={}'.format(bfrdir))
        # Generate new output directory:
        outputdir = '/scratch/data/{}'.format(datadir)
        for host in hosts:
            cmd = ['ssh', host, 'mkdir', '-p', '-m', '1777', outputdir]
            subprocess.run(cmd)
        self.redis_server.publish(group_chan, 'OUTDIR={}'.format(outputdir))
        self.redis_server.publish(group_chan, 'INPUTDIR={}'.format(inputdir))
        log.info('Processing: \ninputdir: {}\noutputdir: {}'.format(inputdir, outputdir))

        # Initiate and track processing by file:
        for rawfile in rawfiles_only:
            log.info('Processing file: {}'.format(rawfile))
            self.redis_server.publish(group_chan, 'RAWFILE={}'.format(rawfile))
            # Wait for processing to start:
            result = self.monitor_proc_status('START', proc_domain, hosts, self.PROC_STATUS_KEY, 600, group_chan)
            if(result == 'timeout'):
                log.error('Timed out, processing has not started')
            # Waiting for processing to finish:
            result = self.monitor_proc_status('END', proc_domain, hosts, self.PROC_STATUS_KEY, 600, group_chan)
            if(result == 'timeout'):
                log.error('Timed out, still waiting for processing to finish')
            # Set procstat to IDLE:
            self.redis_server.publish(group_chan, 'PROCSTAT=IDLE')
            # Uncomment to run slurm commands
            # outcome = slurm_cmd(proc_script)
            log.info('Would run slurm commands here.')
        log.info('Processing complete. Leaving gateway groups.')
        self.redis_server.publish(group_chan, 'leave={}'.format(subarray))


    def monitor_proc_status(self, status, domain, proc_list, proc_key, proc_timeout, group_chan):
        """For processes which communicate via the Hashpipe-Redis Gateway. 
           Monitors a particular key in the satus hash.

          Args: 
               status (str): Status value for 'success' condition. 
               domain (str): Processing domain (for processing nodes). 
               proc_list (list of str): List of host names for processing nodes.
               proc_key (str): Specific HKEY for status monitoring. 
               proc_timeout (int): Time (in seconds) after which the monitor gives up. 
               group_chan (str): Processing group channel.
    
           Returns:
               'success' if all processing nodes exhibit desired status for proc_key.
               'timeout' if processing nodes have not agreed before proc_timeout seconds
               have passed.  
               NOTE: for now, only checks timer when hash is altered. 
        """
        ps = self.redis_server.pubsub()
        proc_status_hash = '{}://{}/0/status'.format(domain, proc_list[0])
        ps.subscribe('__keyspace@0__:{}'.format(proc_status_hash))
        tstart = time.time()
        for msg in ps.listen():
           if(msg['data'] == 'hset'):
               # Since keyspace monitoring is not granular at the hkey level:
               proc_status = self.redis_server.hget(proc_status_hash, proc_key)
               if(proc_status == status):
                   # Check others:
                   full_status = self.gather_proc_status(status, 3, 1, domain, proc_list, proc_key, 3)
                   if(full_status == 'busy'):
                       log.info('full status = busy')
                   elif(full_status == 'done'):
                       log.info('Upchanneliser/beamformer at {}'.format(status))
                       ps.unsubscribe(msg['channel'])
                       return 'success'
               if((time.time() - tstart) >= proc_timeout):
                   log.error('Timeout of {} seconds exceeded'.format(proc_timeout))
                   return 'timeout'
    
    def gather_proc_status(self, status, retries, timeout, domain, proc_list, proc_key, stragglers):
        """Gather aggregated processing status from across hosts. 
    
        Args:
           status (str): Status value for 'success' condition. 
           domain (str): Processing domain (for processing nodes). 
           proc_list (list of str): List of host names for processing nodes.
           proc_key (str): Specific HKEY for status monitoring. 
           timeout (int): Time (in seconds) to wait between retries.  
           retries (int): Number of retries before aborting. 
           stragglers (int): Consider processing complete if at least (n - stragglers)            
           processing nodes are done.

        Returns:
           'busy' if retries exhausted. 
           'done' if all processing nodes indicate the desired success state.
        """
        for i in range(retries):
            proc_count = 0
            for host in proc_list:
                proc_status_hash = '{}://{}/0/status'.format(domain, host)
                proc_status = self.redis_server.hget(proc_status_hash, proc_key)
                if(proc_status == status):
                    proc_count += 1
            if(proc_count < len(proc_list) - 3):
                if(i < retries - 1):
                    log.info('Incomplete agreement of PROCSTAT across hosts. Retrying in {}s.'.format(timeout))
                    time.sleep(timeout)
                else:
                    log.info('Processing incomplete')
                    return 'busy'
            else:
                if(proc_count < len(proc_list)):
                   log.warning('{} straggler(s).'.format(len(proc_list) - proc_count))
                log.info('Gathered proc status: {}'.format(proc_status))
                return 'done'


