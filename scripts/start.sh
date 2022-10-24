#!/bin/bash -e
SCRIPTS_DIR=$(dirname $0)
$SCRIPTS_DIR/check_env.sh
USER=`whoami`
MESSAGE="$USER is manually starting the automator."
python $SCRIPTS_DIR/publish_to_slack.py --slack_channel=meerkat-obs-log --message="$MESSAGE"
$SCRIPTS_DIR/circus.sh start automator
