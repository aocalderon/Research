#!/bin/bash

CORES=144
MFS=( $((1*CORES)) $((2*CORES)) $((3*CORES)) $((4*CORES)) $((5*CORES)) )
DS=( 1000 1500 2000 2500 3000 3500 4000 )

for n in {1..$1}; do
    for MF in ${MFS[@]}; do
	for D in ${DS[@]}; do
	    ./MF_LA25K.sh 20 3 $MF $D
	done
    done
done
