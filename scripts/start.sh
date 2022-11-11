#!/bin/bash -e
SCRIPTS_DIR=$(dirname $0)
PROCESS_NAME=$1
$SCRIPTS_DIR/check_env.sh

if [ "$PROCESS_NAME" != "coordinator" ] && [ "$PROCESS_NAME" != "automator" ] 
then
    echo "Process can be either: automator OR coordinator"
    exit 0
fi

USER=`whoami`
MESSAGE="$USER is manually starting the $PROCESS_NAME."
python $SCRIPTS_DIR/publish_to_slack.py --slack_channel=meerkat-obs-log --message="$MESSAGE"
$SCRIPTS_DIR/circus.sh start $PROCESS_NAME
