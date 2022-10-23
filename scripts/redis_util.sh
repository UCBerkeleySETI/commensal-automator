#!/bin/bash -e
SCRIPTS_DIR=$(dirname $0)
BASE_DIR=$(dirname $SCRIPTS_DIR)
$SCRIPTS_DIR/check_env.sh
cd $BASE_DIR
python -m automator.redis_util $*
