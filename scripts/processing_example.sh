#!/bin/sh
#SBATCH --partition allnodes --time=10:00 --job-name='Example Job'
srun /opt/virtualenv/bluse3/bin/python3.5 /home/obs/bin/publish_to_slack.py --redis_host=10.98.80.10 --proxy_channel=slack-messages --slack_channel=proxy-test --message="starting processing"
srun /opt/virtualenv/bluse3/bin/python3.5 /home/obs/bin/publish_to_slack.py --redis_host=10.98.80.10 --proxy_channel=slack-messages --slack_channel=proxy-test --message="rsync for $(hostname)"
srun -lw rsync -au --info=stats1 "$@" /buf0ro/ /mydatag | sort -sn
srun -lw bash -c 'chmod 1777 /mydatag/20??????/????' | sort -sn
srun /opt/virtualenv/bluse3/bin/python3.5 /home/obs/bin/publish_to_slack.py --redis_host=10.98.80.10 --proxy_channel=slack-messages --slack_channel=proxy-test --message="Additional processing step would go here (e.g. incoherent sum)"
srun /opt/virtualenv/bluse3/bin/python3.5 /home/obs/bin/publish_to_slack.py --redis_host=10.98.80.10 --proxy_channel=slack-messages --slack_channel=proxy-test --message="Additional processing step would go here (e.g. TurboSETI)"
srun /opt/virtualenv/bluse3/bin/python3.5 /home/obs/bin/publish_to_slack.py --redis_host=10.98.80.10 --proxy_channel=slack-messages --slack_channel=proxy-test --message="NVMe buffers (/buf0) would be emptied now."
srun /opt/virtualenv/bluse3/bin/python3.5 /home/obs/bin/publish_to_slack.py --redis_host=10.98.80.10 --proxy_channel=slack-messages --slack_channel=proxy-test --message="The automator would be informed that processing has completed."



