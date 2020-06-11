#!/usr/bin/env bash

# Exit on error
set -e

dir=`dirname "$0"`
cd "$dir"

source "env.sh"

docker-compose -f docker-compose.yml -p mongoeye_mongo stop
docker-compose -f docker-compose.yml -p mongoeye_mongo rm -vf