#!/bin/bash

# Exit on error
set -e

# Stop Xvfb on exit
clean_up () {
    CODE=$?
    start-stop-daemon --verbose --stop  --pidfile /tmp/custom_xvfb_99.pid || true
    exit "${CODE}"
}
trap clean_up EXIT

# Get script directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "${DIR}"

# Env
export width="1440"
export height="920"
export geometry="${width}x${height}"
export DISPLAY=:99
export XENVIRONMENT=./.Xdefaults

# Run Xvfb
start-stop-daemon --verbose --start --pidfile /tmp/custom_xvfb_99.pid --make-pidfile --background --exec /usr/bin/Xvfb -- :99 -once -nocursor -dpms -ac -screen 0 "${geometry}x24+32"
sleep 3

# Hide mouse
unclutter -idle 0.01 -root

# Run demo
termCmd="urxvt -geometry ${geometry} -e ./demo.sh"
echo "Running: ${termCmd}"
byzanz-record -v -w "${width}" -h "${height}" -x 0 -y 0 -c -e "${termCmd}" "${DIR}/../../_release/demo.gif"
