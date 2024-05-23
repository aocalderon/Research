#!/bin/bash


N=$1

DATASET="PFlock/LA/LA_25K"
ENDTIME=25
ES=( 10 15 20 ) # epsilon
CS=( 100 200 500 1000 2000 3000 4000 5000) # capacity

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
