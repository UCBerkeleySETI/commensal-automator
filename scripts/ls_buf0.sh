#!/bin/bash -e
# Check if there are files in the /buf0ro directories.

COUNTER=0
for x in `seq 0 63`; do
    HOST=blpn$x
    for d in `ssh $HOST "find /buf0ro -mindepth 1 -maxdepth 1"`; do
	echo $HOST:$d
	let COUNTER=COUNTER+1
    done
done

echo $COUNTER directories found
