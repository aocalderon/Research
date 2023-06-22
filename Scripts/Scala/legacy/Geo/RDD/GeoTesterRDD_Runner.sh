#!/bin/bash

N=10
MU=3
EPSILON=45
CORES=108
#PS=( $((1*CORES)) $((2*CORES)) $((3*CORES)) $((4*CORES)) )
#QS=( $((1*CORES)) $((2*CORES)) $((3*CORES)) $((4*CORES)) )
IS=( "none" "quadtree" )
GS=( "kdbtree" "quadtree" )

for n in `seq 1 $N`; do
    for I in ${IS[@]}; do
	for G in ${GS[@]}; do
	    ./GeoTesterRDD.sh $EPSILON $MU $I $G
	done
    done
done

