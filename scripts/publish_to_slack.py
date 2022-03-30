"""Script that publishes a desired message to Slack via the slack proxy of the
`meerkat-backend-interface`.
"""

import redis
import argparse
import sys

def cli(args = sys.argv[0]):
    """CLI for slack message publication script.
    """
    usage = "{} [options]".format(args)
    description = 'Publish a message to Slack'
    parser = argparse.ArgumentParser(prog = 'publish_to_slack',
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
    parser.add_argument('--message',
                        type = str,
                        default = 'test_slack_message',
                        help = 'Slack message to be published')
    if(len(sys.argv[1:])==0):
        parser.print_help()
        parser.exit()
    args = parser.parse_args()
    main(proxy_channel = args.proxy_channel,
         slack_channel = args.slack_channel,
         message = args.message,
         redis_host = args.redis_host)

def main(proxy_channel, slack_channel, message, redis_host):
    """Publishes a message to Slack. 

    Args:

        proxy_channel (str): The Redis channel to which the Slack proxy 
        process is listening. 
        slack_channel (str): The Slack channel to which messages should be
        published.
        message (str): The message to be published. 
        redis_host (str): The host address of the Redis server. 

    Returns:

        None
    """ 
    redis_server = redis.StrictRedis(host=redis_host)
    redis_server.publish(proxy_channel, '{}:{}'.format(slack_channel, message))

if(__name__ == '__main__'):
    cli()
