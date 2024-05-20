#!/bin/bash


N=$1

DATASET="PFlock/LA/LA_50K"
ENDTIME=30
ES=( 4 6 8 ) # epsilon
CS=( 75 100 200 500 1000 2000 3000 ) # capacity

for n in `seq 1 $N`
do
    for E in "${ES[@]}"
    do
	for C in "${CS[@]}"
	do
	    echo "Running $n..."
	    echo "./tester_spark --dataset $DATASET --endtime $ENDTIME --method PSI --epsilon $E --mu 3 --capacity $C --fraction 0.5 --master yarn"
	    ./tester_spark --dataset $DATASET --endtime $ENDTIME --method PSI --epsilon $E --mu 3 --capacity $C --fraction 0.5 --master yarn
	done
    done
done
