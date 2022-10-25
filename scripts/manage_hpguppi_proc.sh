#!/bin/bash -e
SCRIPTS_DIR=$(dirname $0)

for x in `seq 0 63`; do
    echo issuing $1 to hpguppi_proc jobs on $x
    circusctl --endpoint tcp://blpn$x:5555 $1 blproc_hashpipe
    circusctl --endpoint tcp://blpn$x:5555 $1 blproc_redisgw
done

