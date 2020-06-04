#!/bin/bash

N=$1
MS=( 'Baseline' 'Index' 'Partition' )
ES=( 10 20 30 40 )
PS=( 16 32 64 128 256 )

for i in $(seq 1 $N); do
    for m in "${MS[@]}"; do
	for e in "${ES[@]}"; do
	    for p in "${PS[@]}"; do
		./DiskFinderTest_Cluster.sh $m $e $p 200 0.025 5
	    done
	done
    done    
done    
