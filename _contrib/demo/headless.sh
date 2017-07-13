#!/bin/bash

set -e

# Get script directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "${DIR}"

# Run headless xserver
termCmd="unclutter -idle 0 -root & lilyterm -e ./make-gif.sh -u lilyterm.conf -s --geometry '880x530+0+0' 2>/dev/null"
xvfb-run -n 99 --server-args='-once -nocursor -dpms -ac -screen 0 880x530x16' bash -c "${termCmd}"