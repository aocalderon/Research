#!/bin/bash

FF=( 32 64 128 256 )
MF=( 1000 1250 1500 1750 2000 2250 2500 2750 3000 )

for F in ${FF[@]}; do
    for M in ${MF[@]}; do
	./FF_LA25K.sh 20 3 3 $F $M 10 100 0.1
    done
done
