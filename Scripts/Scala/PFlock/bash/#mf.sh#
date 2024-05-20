#!/bin/bash


N=$1

DATASET="PFlock/LA/LA_25K"
ENDTIME=15
CS=( 25 50 75 100 200 500 1000 2000 3000 ) # epsilon

for n in `seq 1 $N`
do
	for C in "${CS[@]}"
	do
		echo "Running $n..."
		echo "./tester_spark --dataset $DATASET --endtime $ENDTIME --method PSI --epsilon 15 --mu 3 --capacity $C --fraction 0.5 --master yarn"
		./tester_spark --dataset $DATASET --endtime $ENDTIME --method PSI --epsilon 15 --mu 3 --capacity $C --fraction 0.5 --master yarn
	done
done
