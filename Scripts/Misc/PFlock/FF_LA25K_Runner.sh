#!/bin/bash

FF=( 60 80 100 120 140 160 180 )
MF=( 800 900 950 1000 1050 1100 1200 )

for F in ${FF[@]}; do
    for M in ${MF[@]}; do
	./FF_LA25K.sh 20 3 3 $F $M 10 100 0.1
    done
done
