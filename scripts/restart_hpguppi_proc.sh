#!/bin/bash -e
SCRIPTS_DIR=$(dirname $0)
$SCRIPTS_DIR/multicircus.sh stop blproc_hashpipe
$SCRIPTS_DIR/multicircus.sh stop blproc_redisgw
$SCRIPTS_DIR/multicircus.sh start blproc_hashpipe
$SCRIPTS_DIR/multicircus.sh start blproc_redisgw

