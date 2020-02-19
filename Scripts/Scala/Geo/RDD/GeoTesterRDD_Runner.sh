#!/bin/bash

N=$1
EPSILON=20
CORES=108
PS=( 54 $CORES $((2*CORES)) $((4*CORES)) $((6*CORES)) $((8*CORES)) $((10*CORES)) )
DS=( 1 54 108 162 216 324 )
IS=( "quadtree" "none")

for n in `seq 1 $N`; do
    for P in ${PS[@]}; do
	for D in ${DS[@]}; do
	    for I in ${IS[@]}; do
		./GeoTesterRDD.sh $EPSILON $P $D quadtree $I
	    done
	done
    done
done

