#!/bin/bash

set -e

# ttf-liberation

# Get script directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "${DIR}"

byzanz-record -v -w 880 -h 530 -x 0 -y 0 -c -e ./demo.sh "${DIR}/../../_release/demo.gif"
