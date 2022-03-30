"""Placeholder example of an external script that might be called when the
automator is in the processing state. 

Requires that the `slack_proxy` process of the `meerkat-backend-interface` is
running. 
"""

import redis
import argparse
import sys

def cli(args = sys.argv[0]):
    """CLI for placeholder example script.
    """
    usage = "{} [options]".format(args)
    description = 'Publish subarray details to Slack'
    parser = argparse.ArgumentParser(prog = 'example_placeholder', 
                                     usage = usage, 
                                     description = description)
    parser.add_argument('--redis_host',
                        type = str,
                        default = '127.0.0.1',
                        help = 'Redis server host address.')
    parser.add_argument('--proxy_channel',
                        type = str,
                        default = 'slack-messages',
                        help = 'Redis channel for the slack proxy process.')
    parser.add_argument('--slack_channel',
                        type = str,
                        default = 'proxy-test',
                        help = 'Slack channel to publish messages to.')
    parser.add_argument('--subarray_name',
                        type = str,
                        default = 'test_subarray',
                        help = 'Name of the current active subarray.')
    if(len(sys.argv[1:])==0):
        parser.print_help()
        parser.exit()
    args = parser.parse_args()
    main(proxy_channel = args.proxy_channel, 
         slack_channel = args.slack_channel, 
         subarray_name = args.subarray_name,
         redis_host = args.redis_host)

def main(proxy_channel, slack_channel, subarray_name, redis_host):
    """Publishes a message to Slack. 

    Args:

        proxy_channel (str): The Redis channel to which the Slack proxy 
        process is listening. 
        slack_channel (str): The Slack channel to which messages should be
        published.
        subarray_name (str): The current active subarray for which the Slack
        messages are to be published.
        redis_host (str): The host address of the Redis server. 

    Returns:

        None
    """         
    redis_server = redis.StrictRedis(host=redis_host) 
    slack_message = ('{}:```Automator:\n    Processing placeholder for {}.\n'
        '    Processing would take place now.```').format(slack_channel,
        subarray_name)
    redis_server.publish(proxy_channel, slack_message)

if(__name__ == '__main__'):
    cli()
