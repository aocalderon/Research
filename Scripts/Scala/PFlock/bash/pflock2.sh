#!/bin/bash


N=$1

DATASET="PFlock/Berlin/berlin0-10.tsv"
ES=( 20 30 40 ) # epsilon

for n in `seq 1 $N`
do
	for E in "${ES[@]}"
	do
		echo "Running..."
		echo "./pflock2_spark --dataset $DATASET --sdist 40 --epsilon $E --mu 3 --delta 3 --method PSI --master yarn --capacity 50 --fraction 0.1"
		./pflock2_spark --dataset $DATASET --sdist 40 --epsilon $E --mu 3 --delta 3 --method PSI --master yarn --capacity 50 --fraction 0.1
	done
done
