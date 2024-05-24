#!/bin/bash


N=$1

DATASET="PFlock/LA/LA_25K"
FRACTION=0.1
CS=( 250 500 1000 2000 5000 10000 20000 )
ES=( 10 15 20 ) # epsilon
SD=( 5 7.5 10 ) # speed distance

for n in `seq 1 $N`
do
    for C in "${CS[@]}"
    do
	for e in "${!ES[@]}"
	do
	    echo "PFlock2 Running $n..."
	    echo "./pflock2_spark --dataset $DATASET --sdist ${SD[e]} --epsilon ${ES[e]} --mu 3 --delta 3 --method PSI --master yarn --capacity $C --fraction 0.5"
	    ./pflock2_spark --dataset $DATASET --sdist ${SD[e]} --epsilon ${ES[e]} --mu 3 --delta 3 --method PSI --master yarn --capacity $C --fraction $FRACTION
	done
    done
done
