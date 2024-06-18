#!/bin/bash


N=$1

DATASET="PFlock/LA_25K"
FRACTION=0.1
ENDTIME=60
CS=( 4750 ) 
ES=( 5 10 15 20 25 30 ) # epsilon
SD=( 2.5 5 7.5 10 12.5 15 ) # speed distance
SS=( 6 ) # step

for n in `seq 1 $N`
do
    for e in "${!ES[@]}"
    do
	for S in "${SS[@]}"
	do
	    for C in "${CS[@]}"
	    do
		echo "PFlock4 Running... $n"
		echo "./pflock4_spark --dataset $DATASET --sdist ${SD[e]} --epsilon ${ES[e]} --mu 3 --delta 3 --method PSI --master yarn --capacity $C --fraction $FRACTION --endtime $ENDTIME"
		./pflock4_spark --dataset $DATASET --sdist ${SD[e]} --epsilon ${ES[e]} --mu 3 --delta 3 --method PSI --master yarn --capacity $C --fraction $FRACTION --endtime $ENDTIME
	    done
	done
    done
done
