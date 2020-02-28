#!/bin/bash

N=$1
MU=3
ES=( 15 30 45 )
CORES=108
PS=( $((1*CORES)) $((2*CORES)) $((3*CORES)) $((4*CORES)) )
QS=( $((1*CORES)) $((2*CORES)) $((3*CORES)) $((4*CORES)) )

for n in `seq 1 $N`; do
    for P in ${PS[@]}; do
	for Q in ${QS[@]}; do
	    for E in ${ES[@]}; do
		./GeoTesterRDD.sh $E $MU $P $Q
	    done
	done
    done
done

