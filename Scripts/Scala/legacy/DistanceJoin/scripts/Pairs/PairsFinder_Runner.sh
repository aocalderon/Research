#!/bin/bash

N=$1

CORES=1
EPSILON=10
FRACTION=1
LEVELS=1
THRESHOLD=1
PARTITIONS=1
SCRIPT="PairsFinder.sh"

MS=( 'Baseline' 'Index' )

for i in $(seq 1 $N); do
    for m in "${MS[@]}"; do
	./${SCRIPT} -m $m 
    done    
done    

###

MS=( 'Partition' )
AS=( 10 50 100 250 500 750 1000 )

for i in $(seq 1 $N); do
    for m in "${MS[@]}"; do
	for a in "${AS[@]}"; do
	    ./${SCRIPT} -m $m -a $a
	done
    done    
done    
