#!/bin/bash

# Get script directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Include testing environment
source "${DIR}/../docker/env.sh"

# Add release to PATH
PATH="${DIR}/../../_release/mongoeye/linux/amd64:${PATH}"

# Include demo-magic from https://github.com/paxtonhare/demo-magic
. "${DIR}/demo-magic.sh"

# Custom functions
run() {
    pe "mongoeye ${TEST_MONGO_HOST} ${DEMO_DB} ${DEMO_COL} $1"
}

example () {
    clear
    p "${BOLD}Example $1:${NORMAL} $2"
    sleep 0.7
}

comment () {
    echo
    sleep 0.5
    p "${BOLD}Comment:${NORMAL} $1"
}

pauseAfterExample() {
    sleep "${PAUSE_AFTER_EXAMPLE}"
}

# Configuration
BOLD=$(tput bold)
NORMAL=$(tput sgr0)
TYPE_SPEED=10
NO_WAIT=true
DEMO_DB=db
DEMO_COL=col
PAUSE_AFTER_EXAMPLE=7

# Clear
clear
sleep 5

# EXAMPLE 1
example 1 'Basic usage'
run
comment 'The default scope is random sample of 1000 documents.'
pauseAfterExample

# EXAMPLE 2
example 2 'Analyze in the database with the aggregation framework'
run '--use-aggregation'
comment "By default, analysis runs locally."
comment "Flag ${BOLD}--use-aggregation${NORMAL} serve to run analysis directly in database."
pauseAfterExample

# EXAMPLE 3
example 3 'Analysis of all documents'
run '--scope all'
comment "Flag ${BOLD}--scope all${NORMAL} can be used to analyze all documents."
pauseAfterExample

# EXAMPLE 4
example 4 'Analysis of first 100 documents'
run '--scope first:100'
comment "For all supported scopes, see the help: ${BOLD}mongoeye --help${NORMAL}"
pauseAfterExample

# EXAMPLE 5
example 5 'YAML output'
run '--format yaml | head -n 29'
comment "Use flag ${BOLD}--format yaml${NORMAL} to get results in YAML format."
pauseAfterExample

# EXAMPLE 6
example 6 'JSON output piped to external tool'
run "--format json | jq '.fields[] | select(.name == \"rating\")'"
comment "JSON output can be easily processed by external tools."
pauseAfterExample

# EXAMPLE 7
example 7 'The frequency of values'
run "--most-freq 3 --least-freq 2 --format json |\njq '.fields[] | select(.name == \"rating\") | .types[] | select(.type == \"double\")'"
comment "Flags ${BOLD}--most-freq N${NORMAL} and ${BOLD}--least-freq N${NORMAL} return the most and least frequent values."
pauseAfterExample

# EXAMPLE 8
example 8 'Histogram of values'
run "--value-hist --value-hist-steps 10 --format json |\njq '.fields[] | select(.name == \"rating\") | .types[] | select(.type == \"double\") | .valueHistogram'"
comment "Flag ${BOLD}--value-hist-steps N${NORMAL} set the maximum number of steps. Real step value is rounded."
pauseAfterExample

# EXAMPLE 9
example 9 'Histogram of weekdays'
run "--weekday-hist --format json |\njq '.fields[] | select(.name == \"_id\") | .types[]'"
comment "ObjectId fields are processed as dates."
comment "Flags ${BOLD}--hour-hist${NORMAL} and ${BOLD}--weekday-hist${NORMAL} serve to create a histogram of hours or weekdays."
pauseAfterExample

clear
comment "For other uses, see help: ${BOLD}mongoeye --help${NORMAL}"
pauseAfterExample

# END
wait
