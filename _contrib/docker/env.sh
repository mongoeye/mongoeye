#!/usr/bin/env bash

[[ "${BASH_SOURCE[0]}" != "${0}" ]] || echo "USAGE: source env.sh"

export TEST_MONGO_CONTAINER="mongoeyemongo_test_server_1"
export TEST_MONGO_HOST=`docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ${TEST_MONGO_CONTAINER} 2>/dev/null`
export TEST_MONGO_URI="${TEST_MONGO_HOST}:27017"
export BENCHMARK_MONGO_URI="${TEST_MONGO_HOST}:27017"
export BENCHMARK_DB="company"  # or people, company, restaurant, student
export BENCHMARK_COL="company" # or people, company, restaurant, student
