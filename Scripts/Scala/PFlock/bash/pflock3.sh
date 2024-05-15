#!/bin/bash


N=$1

DATASET="PFlock/Berlin/berlin0-10.tsv"
ES=( 20 30 40 ) # epsilon
SS=( 1 2 3 4 5 6 7 ) # step

for n in `seq 1 $N`
do
	for E in "${ES[@]}"
	do
		for S in "${SS[@]}"
		do
			echo "Running... $n"
			echo "./pflock3_spark --dataset $DATASET --sdist 40 --epsilon $E --mu 3 --delta 3 --method PSI --master yarn --capacity 50 --fraction 0.1 --step $S"
			./pflock3_spark --dataset $DATASET --sdist 40 --epsilon $E --mu 3 --delta 3 --method PSI --master yarn --capacity 50 --fraction 0.1 --step $S
		done
	done
done
