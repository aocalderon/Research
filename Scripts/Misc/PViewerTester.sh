#!/bin/bash

SPATIAL=$1
PSTART=$2
PSTEP=$3
PEND=$4

for PARTITION in `seq $PSTART $PSTEP $PEND`; do
    for NODE in `seq 1 3`; do
	spark-submit --class PartitionViewer /home/acald013/Research/Scripts/Scala/PLCM/target/scala-2.11/plcm_2.11-0.1.jar --input ~/Research/tmp/P_N${NODE}_E110.0_T2.tsv --epsilon 110 --spatial QUADTREE --partitions $PARTITION
    done
done
