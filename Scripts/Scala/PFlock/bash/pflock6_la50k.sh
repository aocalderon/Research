#!/bin/bash


N=$1

DATASET="PFlock/LA_50K"
FRACTION=0.05
ENDTIME=60
#CS=( 30000 16000 10500 7535 6175 5125 ) 
CS=( 4512 4025 ) 
ES=( 20 ) # epsilon
SD=( 10 ) # speed distance
SS=( 2 4 6 8 10 12 14 ) # step

for n in `seq 1 $N`
do
    for e in "${!ES[@]}"
    do
	for S in "${SS[@]}"
	do
	    for C in "${CS[@]}"
	    do
		echo "PFlock6 Running... $n/$N"
		echo "./pflock6_spark --dataset $DATASET --sdist ${SD[e]} --epsilon ${ES[e]} --mu 3 --delta 3 --method PSI --master yarn --capacity $C --fraction $FRACTION --endtime $ENDTIME --step $S"
		./pflock6_spark --dataset $DATASET --sdist ${SD[e]} --epsilon ${ES[e]} --mu 3 --delta 3 --method PSI --master yarn --capacity $C --fraction $FRACTION --endtime $ENDTIME --step $S
 	    done
	done
    done
done
