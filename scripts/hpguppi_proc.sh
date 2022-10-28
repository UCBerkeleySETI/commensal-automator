#!/bin/bash -e
SCRIPTS_DIR=$(dirname $0)
echo issuing $1 to hpguppi_proc services
$SCRIPTS_DIR/multicircus.sh $1 blproc_hashpipe
$SCRIPTS_DIR/multicircus.sh $1 blproc_redisgw
