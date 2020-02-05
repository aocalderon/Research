#!/bin/bash

N=$1
EPSILON=20
CORES=10
PS=( 1 2 4 8 16 32 64 128 256 512 1024 )
GS=( "kdbtree" "quadtree" )
IS=( "rtree" "quadtree" )

for n in `seq 1 $N`; do
    for P in ${PS[@]}; do
	for G in ${GS[@]}; do
	    for I in ${IS[@]}; do
		./GeoTester.sh  $EPSILON $P $G $I
	    done
	done
    done
done

