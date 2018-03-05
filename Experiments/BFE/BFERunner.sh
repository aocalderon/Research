#!/bin/bash

DATASET=$1
ESTART=10
EEND=50
ESTEP=10
MSTART=3
MEND=8
MSTEP=1
DSTART=3
DEND=8
DSTEP=1

for E in `seq $ESTART $ESTEP $EEND` 
do
  for M in `seq $MSTART $MSTEP $MEND` 
  do
    for D in `seq $DSTART $DSTEP $DEND` 
    do
      /opt/BFE_Modified/cmake-build-debug/flocks -m BFE -f $DATASET -d $E -s $M -l $D
    done
  done
done
