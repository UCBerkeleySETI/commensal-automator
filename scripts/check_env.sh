#!/bin/bash -e
# Checks that the python environment is the correct one for running automator scripts.

if [[ `which python` == "/opt/virtualenv/bluse3.9/bin/python" ]]; then
    exit 0
fi

echo "bad python environment. run:"
echo source /opt/virtualenv/bluse3.9/bin/activate
exit 1
