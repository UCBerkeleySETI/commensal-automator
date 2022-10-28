#!/bin/bash -e
SCRIPTS_DIR=$(dirname $0)
cd $SCRIPTS_DIR/..
scripts/check_env.sh
sudo /opt/virtualenv/bluse3.9/bin/python setup.py install
