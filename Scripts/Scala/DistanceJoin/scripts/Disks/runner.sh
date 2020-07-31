#!/bin/bash

CORES=108
FS=( 4 )
PS=( 5 6 7 8 9 10 11 12 13 14 15 )

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


