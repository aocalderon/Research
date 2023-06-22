#!/bin/bash

N=$1

CORES=1
CAPACITY=1
FRACTION=1
LEVELS=1
THRESHOLD=1

PARTITIONS=4

MS=( 'Baseline' 'Index' 'Partition' )
ES=( 10 15 20 )
LS=( 10 15 20 )

for i in $(seq 1 $N); do
    for m in "${MS[@]}"; do
	for e in "${ES[@]}"; do
	    for l in "${LS[@]}"; do
		./DiskFinderTest.sh $m $e $PARTITIONS $CORES $CAPACITY $FRACTION $LEVELS $l $THRESHOLD
	    done
	done
    done    
done    
