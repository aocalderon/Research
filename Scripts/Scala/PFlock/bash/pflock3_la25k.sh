#!/bin/bash


N=$1

DATASET="PFlock/LA_25K"
FRACTION=0.1
ENDTIME=60
CS=( 2300 3000 5000 ) 
ES=( 30 ) # epsilon
SD=( 15 ) # speed distance
SS=( 8 ) # step

for n in `seq 1 $N`
do
    for e in "${!ES[@]}"
    do
	for S in "${SS[@]}"
	do
	    for C in "${CS[@]}"
	    do
		echo "PFlock3 Running... $n"
		echo "./pflock3_spark --dataset $DATASET --sdist ${SD[e]} --epsilon ${ES[e]} --mu 3 --delta 3 --method PSI --master yarn --capacity $C --fraction $FRACTION --endtime $ENDTIME --step $S"
		./pflock3_spark --dataset $DATASET --sdist ${SD[e]} --epsilon ${ES[e]} --mu 3 --delta 3 --method PSI --master yarn --capacity $C --fraction $FRACTION --endtime $ENDTIME --step $S
	    done
	done
    done
done
