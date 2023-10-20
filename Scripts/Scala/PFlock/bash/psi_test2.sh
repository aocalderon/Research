#!/bin/bash


N=$1

DATASET="$HOME/Research/Datasets/dense_recode.tsv"
ES=( 1 2 5 7 10 ) # epsilon
MS=( 3 4 5 6 7  ) # mu

for n in `seq 1 $N`
do
	for e in "${ES[@]}"
	do
		for m in "${MS[@]}"
		do
			echo "Running..."
			echo "/opt/bfe_modified/build/flocks -f $DATASET -d $e -s $m -l 1 -m PSI"
			/opt/bfe_modified/build/flocks -f $DATASET -d $e -s $m -l 1 -m PSI
		done
	done
done
