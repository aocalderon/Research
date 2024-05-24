#!/bin/bash


N=$1

DATASET="PFlock/Berlin/Berlin_10K"
CS=( 100 250 500 1000 2500 5000 10000 )
ES=( 30 40 50 ) # epsilon
SD=( 10 15 15 ) # speed distance

for n in `seq 1 $N`
do
    for C in "${CS[@]}"
    do
	for e in "${!ES[@]}"
	do
	    echo "PFlock2 Running $n..."
	    echo "./pflock2_spark --dataset $DATASET --sdist ${SD[e]} --epsilon ${ES[e]} --mu 3 --delta 3 --method PSI --master yarn --capacity $C --fraction 0.5"
	    ./pflock2_spark --dataset $DATASET --sdist ${SD[e]} --epsilon ${ES[e]} --mu 3 --delta 3 --method PSI --master yarn --capacity $C --fraction 0.5
	done
    done
done
