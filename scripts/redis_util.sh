#!/bin/bash -e
SCRIPTS_DIR=$(dirname $0)
$SCRIPTS_DIR/check_env.sh
cd $SCRIPTS_DIR/..
python -m automator.redis_util $*
