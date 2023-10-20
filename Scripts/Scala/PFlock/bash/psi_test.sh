#!/bin/bash


N=$1

SCRIPT_PATH="$HOME/Research/Scripts/Scala/PFlock/bash/checker_scala"
DATASET="$HOME/Research/Datasets/dense_recode.tsv"
ES=( 1 2 3 4 5 ) # epsilon
MS=( 3 ) # mu

for n in `seq 1 $N`
do
	for e in "${ES[@]}"
	do
		for m in "${MS[@]}"
		do
			echo "Running..."
			echo "$SCRIPT_PATH --dataset $DATASET --epsilon $e --mu $m --tester"
			$SCRIPT_PATH --dataset $DATASET --epsilon $e --mu $m --tester

			echo "/opt/bfe_modified/build/flocks -f $DATASET -d $e -s $m -l 1 -m PSI"
			/opt/bfe_modified/build/flocks -f $DATASET -d $e -s $m -l 1 -m PSI
			/opt/bfe_modified/build/flocks -f $DATASET -d $e -s $m -l 1 -m BFE
		done
	done
done
