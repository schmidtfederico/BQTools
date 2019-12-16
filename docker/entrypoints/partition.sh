#!/bin/bash
set -e

if [ "$1" = 'bash' ]; then
    exec "/bin/bash"
fi

if [ "$1" = 'ls' ]; then
    exec "$@"
fi


exec python partition.py "$@"