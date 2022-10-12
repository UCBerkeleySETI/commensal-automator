import redis
import time
import os
import subprocess

from .logger import log

class ProcHpguppi(object):
    """This class controls processing using hpguppi_proc [1], which performs
    upchannelisation and beamforming on recorded raw data. 

    [1] https://github.com/UCBerkeleySETI/hpguppi_proc/
    """

    def __init__(self):
        
        self.redis_server = redis.StrictRedis(decode_responses=True)
        self.PROC_STATUS_KEY = 'PROCSTAT'

    def process(self, proc_domain, hosts, subarray, bfrdir):
        """Processes the incoming data via hpguppi proc.

        Args:
            proc_domain (str): Processing group domain name.
            hosts (List[str]): List of host names associated with the current
            subarray. 
            subarray (str): Name of the current subarray. 
            bfrdir (str): Directory containing the beamformer recipe files 
            associated with the recorded raw data.
        
        Returns:
            None
        """
        # Find list of files:
        datadir = self.redis_server.get('{}:current_sb_id'.format(subarray))

        # Determine input directory:
        # Check for set of files from each node in case one of them
        # failed to record data.
        rawfiles = set() 
        for host in hosts:
            rawfiles = self.redis_server.smembers('bluse_raw_watch:{}'.format(host))
            if(len(rawfiles) > 0):
                break

        if(len(rawfiles) > 0):

            # Temporarily remove full file path:
            rawfiles_only = [os.path.basename(rawfile) for rawfile in rawfiles]
            inputdirs = [os.path.dirname(rawfile) for rawfile in rawfiles] 
            inputdir = inputdirs[0]

            # Create output directory:
            log.info('Creating output directory...')
            fildir = '/scratch/data/{}/hpguppi_beamformer'.format(datadir)
            for host in hosts:
                cmd = ['ssh', host, 'mkdir', '-p', '-m', '1777', fildir]
                subprocess.run(cmd)

            # Set keys to prepare for processing:
            group_chan = '{}:{}///set'.format(proc_domain, subarray)
            self.redis_server.publish(group_chan, 'BFRDIR={}'.format(bfrdir))
            self.redis_server.publish(group_chan, 'OUTDIR={}'.format(fildir))
            self.redis_server.publish(group_chan, 'INPUTDIR={}'.format(inputdir))
            log.info('Processing: \ninputdir: {}\noutputdir: {}'.format(inputdir, fildir))

            # Initiate and track processing by file:
            for rawfile in rawfiles_only:
                log.info('Processing file: {}'.format(rawfile))
                self.redis_server.publish(group_chan, 'RAWFILE={}'.format(rawfile))
                # Wait for processing to start:
                result = self.monitor_proc_status('START', proc_domain, hosts, self.PROC_STATUS_KEY, 600, group_chan)
                if(result == 'timeout'):
                    log.error('Timed out, processing has not started.')
                    return 1
                # Waiting for processing to finish:
                result = self.monitor_proc_status('END', proc_domain, hosts, self.PROC_STATUS_KEY, 6000, group_chan)
                if(result == 'timeout'):
                    log.error('hpguppi_proc processing timed out.')
                    return 1
                # Set procstat to IDLE:
                self.redis_server.publish(group_chan, 'PROCSTAT=IDLE')
            return 0
        else:
            log.info('No data to process')
            return 1

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
