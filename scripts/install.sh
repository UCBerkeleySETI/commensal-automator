#!/bin/bash -e
SCRIPTS_DIR=$(dirname $0)
cd $SCRIPTS_DIR/..
scripts/check_env.sh
sudo python setup.py install
