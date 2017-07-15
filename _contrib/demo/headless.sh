#!/bin/bash

set -e

# Get script directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "${DIR}"

# Geometry
export width="1440"
export height="920"
export geometry="${width}x${height}"

# Run
termCmd="XENVIRONMENT=./.Xdefaults urxvt -geometry ${geometry} -e ./make-gif.sh"
xvfb-run -n 99 --server-args="-once -nocursor -dpms -ac -screen 0 ${geometry}x24+32" bash -c "${termCmd}"