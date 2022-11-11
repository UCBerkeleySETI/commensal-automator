#!/usr/bin/env python

from optparse import OptionParser
import signal
import sys
import logging

from .coordinator import Coordinator
from automator.logger import log, set_logger

def cli(prog = sys.argv[0]):
    """Command line interface. 
    """
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option('-e', '--endpoint', dest='port', type=str,
                      help='Redis endpoint to connect to (host:port)', default='127.0.0.1:6379')
    parser.add_option('-c', '--config', dest='cfg_file', type=str,
                      help='Config filename (yaml)', default = 'config.yml')
    parser.add_option('-t', '--trigger_mode', dest='trigger_mode', type=str,
                      help="""Trigger mode: 
                                  \'nshot:<n>\': PKTSTART will be sent 
                                  for <n> tracked targets.  
                           """,
                      default = 'nshot:0')
    (opts, args) = parser.parse_args()
    main(port=opts.port, cfg_file=opts.cfg_file, trigger_mode=opts.trigger_mode)

def on_shutdown():
    log.info("Coordinator shutting down.")
    sys.exit()

def main(port, cfg_file, trigger_mode):
    log = set_logger(level=logging.DEBUG)
    coord = Coordinator(port, cfg_file, trigger_mode)
    signal.signal(signal.SIGINT, lambda sig, frame: on_shutdown())
    coord.start()

if(__name__ == '__main__'):
    cli()
