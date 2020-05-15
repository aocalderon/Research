#!/bin/bash

N=$1
MS=( 'Geospark' 'Baseline' 'Index' 'Partition' )
ES=( 10 20 30 )
PS=( 1 4 8 16 )

for i in $(seq 1 $N); do
    for m in "${MS[@]}"; do
	for e in "${ES[@]}"; do
	    for p in "${PS[@]}"; do
		./DiskFinderTest.sh $m $e $p
	    done
	done
    done    
done    
