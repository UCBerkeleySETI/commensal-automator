import redis
import time
import argparse
import sys
import ast

from logger import log, set_logger

"""Initial BLPROC minimal pipeline processing control script.
"""

PROC_STATUS_KEY = 'PROCSTAT'

def proc_sequence(redis_server, domain, subarray_id, proc_script, bfrdir):
    """Processing for minimal BLUSE SETI survey.
    Use this function to run the processing steps for the minimial BLUSE
    survey.

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

    # Starting upchanneliser-beamformer
    # Need to ensure BFRDIR, OUTDIR, RAWFILE are set


    # Detect completion of processing
    monitor_proc_status(domain, redis_server, proc_list)

    # Run slurm command
    outcome = slurm_cmd(proc_script)

    # Final cleanup:
    log.info('Any other final steps go here')


def monitor_proc_status(domain, redis_server, proc_list, proc_key, group_chan):
    """Need to keep track of proc_status key.
       Mechanism: For now, subscribe to one host from proc_list
       and then retrieve others.
    """
    ps = redis_server.pubsub()
    proc_status_hash = '{}://{}/0/status'.format(domain, proc_list[0])
    ps.subscribe('__keyspace@0__:{}'.format(proc_status_hash))

    for msg in ps.listen():
       #log.info(msg['channel'])
       if(msg['data'] == 'hset'):
           # Since keyspace monitoring is not granular at the hkey level:
           proc_status = redis_server.hget(proc_status_hash, proc_key)
           #log.info('hset received')
           if(proc_status == 'END'):
               # Check others:
               log.info('END')
               full_status = gather_proc_status(3, 1, domain, redis_server, proc_list, proc_key)
               if(full_status == 'busy'):
                   log.info('full status = busy')
                   continue
               elif(full_status == 'done'):
                   log.info('Upchanneliser/beamformer finished for all nodes')
                   ps.unsubscribe(msg['channel'])
                   # Set procstat to IDLE
                   redis_server.publish(group_chan, 'PROCSTAT=IDLE')
                   return 'success'


def gather_proc_status(retries, timeout, domain, redis_server, proc_list, proc_key):
    for i in range(retries):
        proc_count = 0
        for host in proc_list:
            proc_status_hash = '{}://{}/0/status'.format(domain, host)
            proc_status = redis_server.hget(proc_status_hash, proc_key)
            log.info('Gathered proc status: {}'.format(proc_status))
            if(proc_status == 'END'):
                log.info('gathered END status')
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

def main(proc_domain, bfrdir, outdir, inputdir, rawfiles, hosts, slurm_script):
    """Run this script separately from the full automator.
    """
    set_logger('DEBUG')

    log.info(hosts)
    log.info(len(hosts))
    log.info(rawfiles)
    log.info(len(rawfiles))
    return



    redis_server = redis.StrictRedis(decode_responses=True)

    # Join gateway groups:
    for host in hosts:
        gateway_chan = '{}://{}/gateway'.format(proc_domain, host)
        redis_server.publish(gateway_chan, 'join=tmp_group')

    # Sleep to let hosts join tmp_group
    time.sleep(1)

    # Initiate processing:
    group_chan = '{}:tmp_group///set'.format(proc_domain)
    redis_server.publish(group_chan, 'BFRDIR={}'.format(bfrdir))
    redis_server.publish(group_chan, 'OUTDIR={}'.format(outdir))
    redis_server.publish(group_chan, 'INPUTDIR={}'.format(inputdir))
    redis_server.publish(group_chan, 'RAWFILE={}'.format(rawfile))

    # Monitor proc status:
    result = monitor_proc_status(proc_domain, redis_server, hosts, PROC_STATUS_KEY, group_chan)

    if(result == 'success'):
        # Run slurm command
        # outcome = slurm_cmd(proc_script)
        log.info('Would run slurm here')
        
    else:
        log.info('Waiting for upchanneliser/beamformer, cannot issue slurm command')

def cli(args = sys.argv[0]):
    """Command line interface for the processing script.
       proc_domain, bfrdir, outdir, rawfile, hostlist, slurm_script
    """
    usage = '{} [options]'.format(args)
    description = 'Run the processing script manually.'
    parser = argparse.ArgumentParser(prog = 'blproc_0',
                                     usage = usage,
                                     description = description)
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
    parser.add_argument('--rawfile',
                        type = str,
                        default = 'guppi_59712_16307_003760_J1939-6342_0001.0000.raw',
                        help = 'Location of the first RAW file to be processed')
    parser.add_argument('--slurm_script',
                        type = str,
                        default = '/opt/virtualenv/bluse3/bin/processing_example.sh',
                        help = 'Location of the slurm processing script.')
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
    main(proc_domain = args.proc_domain,
         bfrdir = args.bfrdir,
         outdir = args.outdir,
         inputdir = args.inputdir,
         rawfiles = args.rawfiles,
         slurm_script = args.slurm_script,
         hosts = args.hosts)

if(__name__ == '__main__'):
  """Run on files locally.
  """
  cli()

