#!/bin/bash

FS=( 0 1 2 3 4 5 )

for n in $(seq 1 $1)
do
    echo "Experiment No. $n"
    echo "./DisksFinder_Control.sh"
    ./DisksFinder_Control.sh
    
    for F in "${FS[@]}"
    do
	echo "./DisksFinder_Factor.sh -f $F"
	./DisksFinder_Factor.sh -f $F
    done
done


