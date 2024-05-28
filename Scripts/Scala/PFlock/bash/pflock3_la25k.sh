#!/bin/bash


N=$1

DATASET="PFlock/LA/LA_25K"
FRACTION=0.1
CS=( 500 1000 2000 ) 
ES=( 20 ) # epsilon
SD=( 10 ) # speed distance
SS=( 1 2 3 4 ) # step

for n in `seq 1 $N`
do
    for e in "${!ES[@]}"
    do
	for S in "${SS[@]}"
	do
	    for C in "${CS[@]}"
	    do
		echo "PFlock3 Running... $n"
		echo "./pflock3_spark --dataset $DATASET --sdist ${SD[e]} --epsilon ${ES[e]} --mu 3 --delta 3 --method PSI --master yarn --capacity $C --fraction $FRACTION --step $S"
		./pflock3_spark --dataset $DATASET --sdist ${SD[e]} --epsilon ${ES[e]} --mu 3 --delta 3 --method PSI --master yarn --capacity $C --fraction $FRACTION --step $S
	    done
	done
    done
done
