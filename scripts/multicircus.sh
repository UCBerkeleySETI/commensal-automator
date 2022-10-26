#!/bin/bash -e
SCRIPTS_DIR=$(dirname $0)

for x in `seq 0 63`; do
    echo issuing $* to circus on blpn$x
    circusctl --endpoint tcp://blpn$x:5555 $*
done

