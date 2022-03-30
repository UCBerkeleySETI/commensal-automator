#!/bin/sh
#SBATCH --partition allnodes --time=10:00 --job-name='Example Job'
srun /opt/virtualenv/bluse3/bin/python3.5 /home/obs/bin/publish_to_slack.py --redis_host=10.98.80.10 --proxy_channel=slack-messages --slack_channel=proxy-test --message=test_message

