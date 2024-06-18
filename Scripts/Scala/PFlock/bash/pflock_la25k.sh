#!/bin/bash


N=$1

DATASET="PFlock/LA_25K"
ENDTIME=60
ES=( 5 10 15 ) # epsilon

for n in `seq 1 $N`
do
    for e in "${!ES[@]}"
    do
	echo "PFlock Running... $n/$N"
	echo "./pflock_spark --dataset $DATASET --epsilon ${ES[e]} --mu 3 --delta 3 --method PSI --master local[*] --endtime $ENDTIME"
	./pflock_spark --dataset $DATASET --epsilon ${ES[e]} --mu 3 --delta 3 --method BFE --master local[*] --endtime $ENDTIME
    done
done
