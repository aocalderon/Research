#!/bin/bash

N=$1
DS=( 2 4 6 8 10 )
ES=( 2 4 6 8 10 ) 

for n in `seq 1 $N`
do
    for D in "${DS[@]}"
    do
	for E in "${ES[@]}"
	do
	    echo "Running $n..."
	    echo "./cmbc2_scala --dataset /home/acald013/Research/Scripts/SUMO/dense/dense_${D}K_crowded_prime.tsv --epsilon $E --mu 3"
	    ./cmbc2_scala --dataset /home/acald013/Research/Scripts/SUMO/dense/dense_${D}K_crowded_prime.tsv --epsilon $E --mu 3
	done
    done
done
