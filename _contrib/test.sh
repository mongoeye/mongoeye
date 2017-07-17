#!/bin/bash

echo "mode: set" > acc.out

fail=false

for dir in $(find . -maxdepth 10 -not -path "./vendor*" -not -path "./.git*" -type d);
do
	if ls ${dir}/*.go &> /dev/null;
	then
	    echo -e "\nPACKAGE: ${dir}"
		richgo test ${dir} -v
		if [[ $? != 0 ]]
		then
    		fail=true
    	fi
    fi
done

# Did a some test fail?
if [[ ${fail} == true ]]
then
    exit 1
else
    echo  -e "\nAll tests passed!\n"
fi
