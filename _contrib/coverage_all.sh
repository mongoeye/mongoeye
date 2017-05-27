#!/bin/bash

echo "mode: set" > acc.out

fail=false

for dir in $(find . -maxdepth 10 -not -path "./vendor*" -not -path "./.git*" -type d);
do
	if ls ${dir}/*.go &> /dev/null;
	then
	    echo -e "\nPACKAGE: ${dir}"
		richgo test ${dir} -v -coverprofile=profile.out
		if [[ $? == 0 ]]
		then
    		if [ -f profile.out ]
    		then
        		cat profile.out | grep -v "mode: set" >> acc.out
    		fi
    	else
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

if [ -n "${COVERALLS}" ]
then
	goveralls -coverprofile=acc.out ${COVERALLS}
fi

rm -rf ./profile.out
rm -rf ./acc.out