import argparse 
import sys

from .automator import Automator
from .logger import log, set_logger

def cli(args = sys.argv[0]):
    """Command line interface for the automator. 
    """
    usage = '{} [options]'.format(args)
    description = 'Start the Commensal Automator'
    parser = argparse.ArgumentParser(prog = 'automator', 
                                     usage = usage, 
                                     description = description)
    parser.add_argument('--redis_endpoint', 
                        type = str,
                        default = '127.0.0.1:6379', 
                        help = 'Local Redis endpoint')
    parser.add_argument('--redis_channel',
                        type = str,
                        default = 'alerts', 
                        help = 'Name of the Redis channel to subscribe to')
    parser.add_argument('--margin',
                        type = float,
                        default = 10.0, 
                        help = 'Safety margin for recording duration (sec)')
    parser.add_argument('--hpgdomain',
                        type = str,
                        default = 'bluse', 
                        help = 'Hashpipe-Redis gateway domain')
    parser.add_argument('--buffer_length',
                        type = float,
                        default = 300.0, 
                        help = 'Max recording length at max data rate (sec)')
    parser.add_argument('--nshot_chan',
                        type = str,
                        default = 'coordinator:trigger_mode', 
                        help = 'Redis channel for changing nshot')
    parser.add_argument('--nshot_msg',
                        type = str,
                        default = 'coordinator:trigger_mode:{}:nshot:{}', 
                        help = 'Format of message for changing nshot')
    if(len(sys.argv[1:]) == 0):
        parser.print_help()
        parser.exit()
    args = parser.parse_args()
    main(redis_endpoint = args.redis_endpoint, 
         redis_channel = args.redis_channel, 
         margin = args.margin, 
         hpgdomain = args.hpgdomain, 
         buffer_length = args.buffer_length, 
         nshot_chan = args.nshot_chan, 
         nshot_msg = args.nshot_msg)

def main(redis_endpoint, redis_channel, margin, hpgdomain, 
    buffer_length, nshot_chan, nshot_msg):
    """Starts the automator.
  
    Args: 

        redis_endpoint (str): Redis endpoint (of the form <host IP
        address>:<port>) 
        redis_chan (str): Name of the redis channel
        proc_env (str): Virtual environment for processing script. 
        proc_args (str): Arguments for processing script. 
        margin (float): Safety margin (in seconds) to add to `DWELL`
        when calculating the estimated end of a recording. 
        hpgdomain (str): The Hashpipe-Redis Gateway domain for the instrument
        in question. 
        buffer_length (float): Maximum duration of recording (in seconds)
        for the maximum possible incoming data rate. 
        nshot_chan (str): The Redis channel for resetting nshot.
        nshot_msg (str): The base form of the Redis message for resetting
        nshot. For example, `coordinator:trigger_mode:<subarray_name>:nshot:<n>`

    Returns:

        None
    """
    set_logger('DEBUG')
    Automaton = Automator(redis_endpoint, 
                          redis_channel, 
                          margin, 
                          hpgdomain, 
                          buffer_length, 
                          nshot_chan, 
                          nshot_msg)
    Automaton.start()

if(__name__ == '__main__'):
    cli() 
