#!/bin/bash


N=$1

DATASET="PFlock/LA_50K"
FRACTION=0.01
ENDTIME=60
CS=( 1000 1500 2000 2500 3000 5000 10000 20000 )
ES=( 10 15 20 ) # epsilon
SD=( 5 7.5 10 ) # speed distance

for n in `seq 1 $N`
do
    for C in "${CS[@]}"
    do
	for e in "${!ES[@]}"
	do
	    echo "PFlock2 Running $n..."
	    echo "./pflock2_spark --dataset $DATASET --sdist ${SD[e]} --epsilon ${ES[e]} --mu 3 --delta 3 --method PSI --master yarn --capacity $C --fraction $FRACTION --endtime $ENDTIME"
	    ./pflock2_spark --dataset $DATASET --sdist ${SD[e]} --epsilon ${ES[e]} --mu 3 --delta 3 --method PSI --master yarn --capacity $C --fraction $FRACTION --endtime $ENDTIME
	done
    done
done
