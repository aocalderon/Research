#!/bin/bash


N=$1

DATASET="PFlock/LA_50K"
FRACTION=0.05
ENDTIME=60
CS=( 7535 ) 
ES=( 4 8 12 16 20 ) # epsilon
SD=( 2 4 6 8 10 ) # speed distance
SS=( 1 ) # step

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
