#!/bin/bash

N=10
MU=3
CORES=108
#PS=( $((1*CORES)) $((2*CORES)) $((3*CORES)) $((4*CORES)) )
#QS=( $((1*CORES)) $((2*CORES)) $((3*CORES)) $((4*CORES)) )
ES=( 10 20 30 40 )
CS=( 200 )
FS=( 0.025 )

for n in `seq 1 $N`; do
    for E in ${ES[@]}; do
	for C in ${CS[@]}; do
	    for F in ${FS[@]}; do
		echo "./GeoTesterRDD_Viz.sh $E $C $F"
		./GeoTesterRDD_Viz.sh $E $C $F
	    done
	done
    done
done

