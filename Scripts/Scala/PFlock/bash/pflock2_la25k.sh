#!/bin/bash


N=$1

DATASET="PFlock/LA_25K"
FRACTION=0.1
ENDTIME=60
CS=( 400 500 700 1000 1300 2000 2300 3000 5000 10000 20000 )
ES=( 20 25 30 ) # epsilon
SD=( 10 12.5 15 ) # speed distance

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