#!/bin/bash

#urxvt

# Get script directory
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "${DIR}"

# Include testing environment
source ../docker/env.sh

# Add release to PATH
PATH="${DIR}/../../_release/mongoeye/linux/amd64:${PATH}"

# Include demo-magic from https://github.com/paxtonhare/demo-magic
. lib/demo-magic.sh

# Custom functions
run() {
    pe "mongoeye ${TEST_MONGO_HOST} ${DEMO_DB} ${DEMO_COL} $1"
}

example () {
    clear
    p "${BOLD}Example $1:${NORMAL} $2"
    sleep 0.5
}

comment () {
    echo
    sleep 0.3
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
PAUSE_AFTER_EXAMPLE=8

# Clear
clear
sleep 0.5

# EXAMPLE 1
example 1 'Basic usage'
run
comment 'The default scope is random sample of 1000 documents.'
pauseAfterExample

# EXAMPLE 2
example 2 'Analysis using the aggregation framework'
run '--use-aggregation'
comment "By default, analysis runs locally."
comment "Option ${BOLD}--use-aggregation${NORMAL} serve to run analysis directly in the database."
pauseAfterExample

# EXAMPLE 3
example 3 'Analysis of all documents'
run '--sample all'
comment "Option ${BOLD}--sample all${NORMAL} can be used to analyze all documents."
pauseAfterExample

# EXAMPLE 4
example 4 'Analysis of first 100 documents'
run '--sample first:100'
comment "For more info, see ${BOLD}--match${NORMAL}, ${BOLD}--sample${NORMAL} and ${BOLD}--project${NORMAL} in the help."
pauseAfterExample

# EXAMPLE 5
example 5 'Analyze only some fields'
run "--project '{\"_id\": 0, \"rating\": 1}'"
comment "Option ${BOLD}--project${NORMAL} can be used to include/exclude fields from analysis."
pauseAfterExample

# EXAMPLE 6
example 6 'YAML output'
run '--format yaml | head -n 29'
comment "Use option ${BOLD}--format yaml${NORMAL} to get results in YAML format."
pauseAfterExample

# EXAMPLE 7
example 7 'JSON output piped to an external tool'
run "--format json | jq '.fields[] | select(.name == \"rating\")'"
comment "JSON output can be easily processed using external tools."
pauseAfterExample

# EXAMPLE 8
example 8 'The frequency of values'
run "--most-freq 3 --least-freq 2 --format json |\njq '.fields[] | select(.name == \"rating\") | .types[] | select(.type == \"double\")'"
comment "Options ${BOLD}--most-freq N${NORMAL} and ${BOLD}--least-freq N${NORMAL} return the most and least frequent values."
pauseAfterExample

# EXAMPLE 9
example 9 'Histogram of value'
run "--value-hist --value-hist-steps 10 --format json |\njq '.fields[] | select(.name == \"rating\") | .types[] | select(.type == \"double\") | .valueHistogram'"
comment "Option ${BOLD}--value-hist-steps N${NORMAL} set the maximum number of steps. Real step value is rounded."
pauseAfterExample

# EXAMPLE 10
example 10 'Histogram of weekday'
run "--weekday-hist --format json |\njq '.fields[] | select(.name == \"_id\") | .types[]'"
comment "ObjectId fields are processed as dates."
comment "Options ${BOLD}--hour-hist${NORMAL} and ${BOLD}--weekday-hist${NORMAL} serve to create a histogram of hour or weekday."
pauseAfterExample

clear
comment "For other uses, see help: ${BOLD}mongoeye --help${NORMAL}"
pauseAfterExample