#!/bin/bash

CORES=12
PS=( 3 4 5 6 7 8 9 10 )

for n in $(seq 1 $1)
do
    echo "Experiment No $n"
    for P in "${PS[@]}"
    do
	echo "./DisksFinder_server.sh -p $((P * CORES)) -d"
	./DisksFinder_server.sh -p $((P * CORES)) -d
    done
done


