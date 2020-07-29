#!/bin/bash

CORES=108
FS=( 1 2 3 4 5 )
PS=( 8 9 10 11 12 )

for n in $(seq 1 $1)
do
    echo "$n"
    for F in "${FS[@]}"
    do
	for P in "${PS[@]}"
	do
	    echo "./DisksFinder_server.sh -p $((P * CORES)) -f $F"
	    ./DisksFinder_server.sh -p $((P * CORES)) -f $F
	done
    done
done


