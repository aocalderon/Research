#!/bin/bash


N=$1

DATASET="PFlock/LA/LA_25K"
ES=( 5 10 15 ) # epsilon
SS=( 1 2 3 4 5 6 7 ) # step

for n in `seq 1 $N`
do
	for E in "${ES[@]}"
	do
		for S in "${SS[@]}"
		do
			echo "PFlock3 Running... $n"
			echo "./pflock3_spark --dataset $DATASET --sdist 10 --epsilon $E --mu 3 --delta 3 --method PSI --master yarn --capacity 100 --fraction 0.01 --step $S"
			./pflock3_spark --dataset $DATASET --sdist 10 --epsilon $E --mu 3 --delta 3 --method PSI --master yarn --capacity 100 --fraction 0.01 --step $S
		done
	done
done
