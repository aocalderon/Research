#!/bin/bash

#FF=( 60 80 100 120 140 160 180 )
MF=( 150 300 450 600 )
ENTRIES=( 25 50 75 )

for M in ${MF[@]}; do
    for E in ${ENTRIES[@]}; do
	./FF_LA25K.sh 20 3 3 120 $M $E 0 2
    done
done
