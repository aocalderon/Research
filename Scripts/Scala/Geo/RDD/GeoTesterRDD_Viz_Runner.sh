#!/bin/bash

N=10
MU=3
EPSILON=45
CORES=108
#PS=( $((1*CORES)) $((2*CORES)) $((3*CORES)) $((4*CORES)) )
#QS=( $((1*CORES)) $((2*CORES)) $((3*CORES)) $((4*CORES)) )
CS=( 1000 2000 3000 4000 5000 )
FS=( 0.01 0.05 0.1 )

for n in `seq 1 $N`; do
    for C in ${CS[@]}; do
	for F in ${FS[@]}; do
	    echo "./GeoTesterRDD_Viz.sh $EPSILON $C $F"
	    ./GeoTesterRDD_Viz.sh $EPSILON $C $F
	done
    done
done

