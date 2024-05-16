#!/bin/bash


N=$1

DATASET="PFlock/LA/LA_25K"
ES=( 5 10 15 ) # epsilon

for n in `seq 1 $N`
do
	for E in "${ES[@]}"
	do
		echo "PFlock2 Running $n..."
		echo "./pflock2_spark --dataset $DATASET --sdist 10 --epsilon $E --mu 3 --delta 3 --method PSI --master yarn --capacity 100 --fraction 0.01"
		./pflock2_spark --dataset $DATASET --sdist 10 --epsilon $E --mu 3 --delta 3 --method PSI --master yarn --capacity 100 --fraction 0.01
	done
done
