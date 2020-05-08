#!/bin/bash

N=$1
MS=( 'Geospark' 'Baseline' 'Index' 'Partition' )
ES=( 10 20 30 40 )
PS=( 4 8 16 32 )

for i in $(seq 1 $N); do
    for m in "${MS[@]}"; do
	for e in "${ES[@]}"; do
	    for p in "${PS[@]}"; do
		./DiskFinderTestDebug.sh $m $e $p 20 0.025 8
	    done
	done
    done    
done    
