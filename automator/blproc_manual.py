import redis
import time
import argparse
import sys
import ast

from .logger import log, set_logger

"""Manual BLPROC minimal pipeline processing control script.
"""

PROC_STATUS_KEY = 'PROCSTAT'

def proc_slurm(redis_server, domain, subarray_id, proc_script, bfrdir, outputdir, inputdir, rawfile):
    """Processing for minimal BLUSE SETI survey.
    For use with processing stages that do not use the Hashpipe-Redis Gateway.

    Currently, adheres to the following sequence:

    1. Knows name of gateway group for current subarray.
    2. Knows list of hosts in current subarray.
    3. Starts upchanneliser-beamformer on recorded HPGUPPI RAW data.
       3a. Retrieves and publishes key-value pairs to initiate start.
       3b. Waits for responses (this can be blocking here).
    4. Starts a SETI search on filterbank output of upchanneliser-beamformer,
       using Slurm.
    """

    proc_group = '{}:{}///set'.format(domain, subarray_id)
    proc_list = retrieve_host_list(subarray_id)

    # Set keys to prepare for processing:
    redis_server.publish(proc_group, 'BFRDIR={}'.format(bfrdir))
    redis_server.publish(proc_group, 'OUTDIR={}'.format(outdir))
    redis_server.publish(proc_group, 'INPUTDIR={}'.format(inputdir))
    
    # Initiate processing with rawfile name
    redis_server.publish(group_chan, 'RAWFILE={}'.format(rawfile))

    # Detect completion of processing
    monitor_proc_status(domain, redis_server, proc_list, PROC_STATUS_KEY, proc_timeout, group_chan)

    # Run slurm command
    outcome = slurm_cmd(proc_script)

    # Final cleanup:
    log.info('Any other final steps go here')
    redis_server.publish(proc_group, 'leave={}'.format(subarray_id))

def monitor_proc_status(status, domain, redis_server, proc_list, proc_key, proc_timeout, group_chan):
    """For processes which communicate via the Hashpipe-Redis Gateway. 
    Monitors a particular key in the satus hash.

       Args:
           status (str): Status value for 'success' condition. 
           redis_server: Redis server via which to access proc hash. 
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
    ps = redis_server.pubsub()
    proc_status_hash = '{}://{}/0/status'.format(domain, proc_list[0])
    ps.subscribe('__keyspace@0__:{}'.format(proc_status_hash))
    tstart = time.time()
    for msg in ps.listen():
       #log.info(msg['channel'])
       if(msg['data'] == 'hset'):
           # Since keyspace monitoring is not granular at the hkey level:
           proc_status = redis_server.hget(proc_status_hash, proc_key)
           if(proc_status == status):
               # Check others:
               full_status = gather_proc_status(status, 3, 1, domain, redis_server, proc_list, proc_key)
               if(full_status == 'busy'):
                   log.info('full status = busy')
               elif(full_status == 'done'):
                   log.info('Upchanneliser/beamformer finished for all nodes')
                   ps.unsubscribe(msg['channel'])
                   return 'success'
           if((time.time() - tstart) >= proc_timeout): 
               log.error('Processing timeout of {} seconds exceeded'.format(proc_timeout))
               return 'timeout'

def gather_proc_status(status, retries, timeout, domain, redis_server, proc_list, proc_key):
    """Gather aggregated processing status from across hosts. 

    Args:
       status (str): Status value for 'success' condition. 
       redis_server: Redis server via which to access proc hash. 
       domain (str): Processing domain (for processing nodes). 
       proc_list (list of str): List of host names for processing nodes.
       proc_key (str): Specific HKEY for status monitoring. 
       timeout (int): Time (in seconds) to wait between retries.  
       retries (int): Number of retries before aborting. 
        
    Returns:
       'busy' if retries exhausted. 
       'done' if all processing nodes indicate the desired success state.
    """ 
    for i in range(retries):
        proc_count = 0
        for host in proc_list:
            proc_status_hash = '{}://{}/0/status'.format(domain, host)
            proc_status = redis_server.hget(proc_status_hash, proc_key)
            log.info('Gathered proc status: {}'.format(proc_status))
            if(proc_status == status):
                proc_count += 1
        if(proc_count != len(proc_list)):
            if(i < retries - 1):
                log.info('Processing incomplete across hosts. Retrying in {}s.'.format(timeout))
                time.sleep(timeout)
            else:
                log.info('Processing incomplete')
                return 'busy'
        else:
            return 'done'

def slurm_cmd(proc_script):
    """Run slurm processing script.

    Args:
        proc_script (str): Path to processing shell script. 
 
    Returns:
        'success' if slurm processing script finished executing. 
        'failed' if the provided script could not be run. 
    """
    slurm_cmd = ['sbatch', '-w', host_list, proc_script]
    log.info('Running processing script: {}'.format(slurm_cmd))
    try:
        subprocess.Popen(slurm_cmd).wait()
        return 'success'
    except:
        log.error('Could not run script for {}'.format(subarray_name))
        return 'failed'

def retrieve_host_list(subarray_id):
    """Retrieve the current list of processing node host names allocated to
    the subarray `subarray_id`.
    """
    host_key = 'coordinator:allocated_hosts:{}'.format(subarray_id)
    host_list = self.redis_server.lrange(host_key,
                                         0,
                                         self.redis_server.llen(host_key))
    # Format for host name (rather than instance name):
    host_list =  [host.split('/')[0] for host in host_list]
    host_list = ','.join(host_list)
    return host_list

def proc_hpguppi(proc_domain, bfrdir, outdir, inputdir, rawfiles, hosts, slurm_script, proc_timeout):
    """Proc sequence for processing steps which use the Hashpipe-Redis Gateway.
       This script runs separately from the full automator.
    """
    set_logger('DEBUG')
    log.info('Starting blproc_manual')
    redis_server = redis.StrictRedis(decode_responses=True)
 
    # Parsing input:
    if(hosts is None):
        log.error('Please provide a list of hosts, or \'all\'')
        sys.exit()
    elif((len(hosts) == 1) & (hosts[0] == 'all')):
        hosts = []
        for i in range(0, 64):
            hosts.append('blpn{}'.format(i))
    if(rawfiles is None): 
        log.error('Please provide a list of rawfiles for processing')
        sys.exit()

    # Join gateway groups:
    for host in hosts:
        gateway_chan = '{}://{}/gateway'.format(proc_domain, host)
        redis_server.publish(gateway_chan, 'join=tmp_group')

    # Sleep to let hosts join tmp_group
    time.sleep(1)

    # Set keys to prepare for processing:
    group_chan = '{}:tmp_group///set'.format(proc_domain)
    redis_server.publish(group_chan, 'BFRDIR={}'.format(bfrdir))
    redis_server.publish(group_chan, 'OUTDIR={}'.format(outdir))
    redis_server.publish(group_chan, 'INPUTDIR={}'.format(inputdir))

    # Initiate and track processing by file:
    for rawfile in rawfiles:
        log.info('Processing file: {}'.format(rawfile))
        redis_server.publish(group_chan, 'RAWFILE={}'.format(rawfile))
        # Wait for processing to start:
        result = monitor_proc_status('START', proc_domain, redis_server, hosts, PROC_STATUS_KEY, proc_timeout, group_chan)
        if(result == 'timeout'):
            log.error('Timed out, processing has not started')
            sys.exit() 
        # Waiting for processing to finish:
        result = monitor_proc_status('END', proc_domain, redis_server, hosts, PROC_STATUS_KEY, proc_timeout, group_chan)
        if(result == 'timeout'):
            log.error('Timed out, still waiting for processing to finish')
            sys.exit()
        # Set procstat to IDLE:
        redis_server.publish(group_chan, 'PROCSTAT=IDLE')
        # Uncomment to run slurm commands
        # outcome = slurm_cmd(proc_script)
        log.info('Would run slurm commands here.')
    log.info('Processing complete. Leaving gateway groups.')
    redis_server.publish(group_chan, 'leave=tmp_group')

def main(proc_type, proc_domain, bfrdir, outdir, inputdir, rawfiles, hosts, slurm_script, proc_timeout):
    """Run processing manually on local files based on processing type. 
    """
    if(proc_type == 'slurm'):
        proc_slurm(proc_domain, bfrdir, outdir, inputdir, rawfiles, hosts, slurm_script, proc_timeout)
    elif(proc_type == 'hpguppi'):
        proc_hpguppi(proc_domain, bfrdir, outdir, inputdir, rawfiles, hosts, slurm_script, proc_timeout)
    else:
        log.error('Unrecognised processing type')

def cli(args = sys.argv[0]):
    """Command line interface for the processing script.
       proc_domain, bfrdir, outdir, rawfile, hostlist, slurm_script
    """
    usage = '{} [options]'.format(args)
    description = 'Run the processing script manually.'
    parser = argparse.ArgumentParser(prog = 'blproc_0',
                                     usage = usage,
                                     description = description)
    parser.add_argument('--proc_type',
                        type = str,
                        default = 'slurm',
                        help = 'Processing approach: slurm or hashpipe.')
    parser.add_argument('--proc_domain',
                        type = str,
                        default = 'blproc',
                        help = 'Domain for processing gateway.')
    parser.add_argument('--bfrdir',
                        type = str,
                        default = '/home/davidm/bfr5.test',
                        help = 'Location of the beamformer recipe file.')
    parser.add_argument('--outdir',
                        type = str,
                        default = '/scratch/test',
                        help = 'Output directory for upchanneliser-beamformer')
    parser.add_argument('--inputdir',
                        type = str,
                        default = '/buf0ro/20220512/0009/Unknown/GUPPI/',
                        help = 'Input directory for upchanneliser-beamformer')
    parser.add_argument('--slurm_script',
                        type = str,
                        default = '/opt/virtualenv/bluse3/bin/processing_example.sh',
                        help = 'Location of the slurm processing script.')
    parser.add_argument('--proc_timeout',
                        type = int,
                        default = 1800,
                        help = 'Upchanneliser-beamformer processing timeout (s)')
    # Optional args
    parser.add_argument('--hosts',
                        nargs='*',
                        action='store',
                        default = None,
                        help = 'Hosts allocated for processing.')
    parser.add_argument('--rawfiles',
                        nargs='*',
                        action='store',
                        default = None,
                        help = 'Rawfiles to be processed.')

    if(len(sys.argv[1:]) == 0):
        parser.print_help()
        parser.exit()
    args = parser.parse_args()
    main(proc_type = args.proc_type,
         proc_domain = args.proc_domain,
         bfrdir = args.bfrdir,
         outdir = args.outdir,
         inputdir = args.inputdir,
         rawfiles = args.rawfiles,
         slurm_script = args.slurm_script,
         proc_timeout = args.proc_timeout,
         hosts = args.hosts)

if(__name__ == '__main__'):
  """Run on files locally.
  """
  cli()

