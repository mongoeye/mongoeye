#!/usr/bin/env bash

# Exit on error
set -e

echo "Starting MongoDB ..."
mongod &

# Wait for MongoDB
printf "\nWaiting until MongoDB become ready "
max_attempts=40
attempt=0
until mongo --quiet --eval "db.version()"  "localhost" > /dev/null 2>&1
do
  if (( attempt > max_attempts )); then
    printf "\nCan't connect to server.\n"
    exit 1
  fi

  printf "."
  sleep 1

  attempt=$((attempt+1))
done
printf "\n\n"

# Print MongoDB version
DB_VERSION=`mongo --quiet --eval "db.version()" "localhost"`
echo "MongoDB started, version: $DB_VERSION"
printf "\n"

# Create admin user if not exists
createAdminUser () {
   printf "\n"
   echo "Creating admin user (admin, 12345), if not exists..."
   mongo --quiet --eval "db = db.getSiblingDB('admin'); if (db.getUsers().length == 0) { db.createUser({ user: 'admin', pwd: '12345', roles:['root'] } ) }"
}

# There is a database of the given name? $dbName
dbExists () {
   echo "Testing if database $1 exists"
   mongo --quiet --eval "db = db.getSiblingDB('admin'); db.runCommand('listDatabases').databases.forEach(function(r){if (r.name == '$1') {quit(0)}}); quit(1)"
}

# Import data from JSON file: $file, $dbName, $colName
importJSON() {
    printf "\n"
    echo  "Importing JSON file $1 to $2.$3"
    cat "$1" | mongoimport --drop -d "$2" -c "$3"
    echo -e "OK\n"
}

# Import first N data from JSON file: $file, $size, $dbName, $colName
importJSONFirstN() {
    printf "\n"
    echo  "Importing JSON first $2 documents from file $1 to $3.$4"
    cat "$1" | jq -s -c -M ".[:$2]" | mongoimport --drop --jsonArray -d "$3" -c "$4"
    echo -e "OK\n"
}

# Import data from archive: $file, $dbName, $colName
importArchive() {
    echo "Importing archive file $1 to $2.$3"
    zcat "$1" | mongorestore --drop --db "$2" --collection "$3" -
}

# Import datasets
dbExists "company"        || importJSON         "/init/dataset/companies.json" "company" "company"
dbExists "company1000"    || importJSONFirstN   "/init/dataset/companies.json" 1000 "company1000" "company1000"
dbExists "company5000"    || importJSONFirstN   "/init/dataset/companies.json" 5000 "company5000" "company5000"
dbExists "restaurant"     || importJSON         "/init/dataset/restaurant.json" "restaurant" "restaurant"
dbExists "student"        || importJSON         "/init/dataset/students.json" "student" "student"
dbExists "people"         || importArchive      "/init/dataset/people.bson.gz" "people" "people"

# Dataset for demo
dbExists "db"     || importJSON "/init/dataset/restaurant.json" "db" "col"

createAdminUser

printf "\nOK\n\n"
