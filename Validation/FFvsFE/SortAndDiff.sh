#!/bin/bash

EPSILON=( 10 20 30 )
MU=( 3 )
DELTA=( 3 )

for E in "${EPSILON[@]}"
do
    for M in "${MU[@]}"
    do
	for D in "${DELTA[@]}"
	do
	    echo "sort /tmp/FE_E${E}_M${M}_D${D}.tsv -o /tmp/FE_E${E}_M${M}_D${D}_sorted.tsv"
	    sort /tmp/FE_E${E}_M${M}_D${D}.tsv -o /tmp/FE_E${E}_M${M}_D${D}_sorted.tsv
	    echo "sort /tmp/FF_E${E}_M${M}_D${D}.tsv -o /tmp/FF_E${E}_M${M}_D${D}_sorted.tsv"
	    sort /tmp/FF_E${E}_M${M}_D${D}.tsv -o /tmp/FF_E${E}_M${M}_D${D}_sorted.tsv

	    echo "diff -s /tmp/FE_E${E}_M${M}_D${D}_sorted.tsv /tmp/FF_E${E}_M${M}_D${D}_sorted.tsv"
	    diff -s /tmp/FE_E${E}_M${M}_D${D}_sorted.tsv /tmp/FF_E${E}_M${M}_D${D}_sorted.tsv
	done
    done
done
			   
			  
