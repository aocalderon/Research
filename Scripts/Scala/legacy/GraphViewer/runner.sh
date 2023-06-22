#!/bin/bash

INPUT="${HOME}/Research/Datasets/dense.tsv"
ES=( 30 )
AS=( 1 2 3 )

for N in $(seq 1 $1)
do
    for E in "${ES[@]}"
    do
	for A in "${AS[@]}"
	do
	    #echo "Run $N: benchmark.sh $INPUT $E $A"
	    ./benchmark.sh $INPUT $E $A
	done
    done
done
