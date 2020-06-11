#!/bin/bash

# Exit on error
set -e

# Hide job control messages
set +m

# Stop Xvfb on exit
clean_up () {
    CODE=$?

    # Kill unclutter
    kill %1 || true
    wait %1 2>/dev/null || true

    # Stop Xvfb
    start-stop-daemon --stop  --pidfile /tmp/custom_xvfb_99.pid

    exit "${CODE}"
}
trap clean_up EXIT

# Get script directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "${DIR}"

# Env
export width="1920"
export height="1200"
export geometry="${width}x${height}"
export background="#0a0a08"
export DISPLAY=:99
export XENVIRONMENT=./.Xdefaults
export NO_AT_BRIDGE=1 # https://gist.github.com/jeffcogswell/62395900725acef1c0a5a608f7eb7a05

# Start Xvfb
start-stop-daemon --start --pidfile /tmp/custom_xvfb_99.pid --make-pidfile --background --exec /usr/bin/Xvfb -- :99 -once -nocursor -dpms -ac -screen 0 "${geometry}x24+32"
sleep 1

# Hide mouse
unclutter -idle 0 -root &
sleep 1

# Set background
xsetroot -solid "${background}"

# Run demo
termCmd="urxvt -geometry ${geometry} -e ./demo.sh"
echo "Running: ${termCmd}"
byzanz-record -v -w "${width}" -h "${height}" -x 0 -y 0 -e "${termCmd}" "${DIR}/../../_release/demo.gif"

# Resize
gifsicle "${DIR}/../../_release/demo.gif" --resize-width 880 > "${DIR}/../../_release/demo-small.gif"
mv "${DIR}/../../_release/demo-small.gif" "${DIR}/../../_release/demo.gif"