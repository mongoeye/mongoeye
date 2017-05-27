#!/bin/sh

for dir in $(find . -maxdepth 10 -not -path "./vendor*" -not -path "./.git*" -not -path "." -type d);
do
    echo "DIR: ${dir}"
	gofmt -s -l -w ${dir}
done
