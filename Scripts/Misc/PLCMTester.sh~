#!/bin/bash

JAR="/home/acald013/Research/Scripts/Scala/PLCM/target/scala-2.11/plcm_2.11-0.1.jar"


PSTART=8
PEND=30
PSTEP=2
SPATIAL="QUADTREE"
N=10

for n in `seq 1 $N`; do
    for p in `seq $PSTART $PSTEP $PEND`; do
	echo "spark-submit ${JAR} --offset 0 --input ~/Research/tmp/toprune_5.tsv --epsilon 100 --spatial ${SPATIAL} --partitions ${p}"
	spark-submit --num-executors 3 --executor-cores 4 ${JAR} --offset 0 --input ~/Research/tmp/toprune_5.tsv --epsilon 100 --spatial ${SPATIAL} --partitions ${p}
    done
done
