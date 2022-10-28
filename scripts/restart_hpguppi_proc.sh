#!/bin/bash -e
SCRIPTS_DIR=$(dirname $0)
$SCRIPTS_DIR/hpguppi_proc.sh stop
$SCRIPTS_DIR/hpguppi_proc.sh start

