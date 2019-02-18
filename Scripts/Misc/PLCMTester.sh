#!/bin/bash

JAR="/home/acald013/Research/Scripts/Scala/PLCM/target/scala-2.11/plcm_2.11-0.1.jar"


CSTART=3
CEND=12
CSTEP=3
SPATIAL="QUADTREE"
PARTITIONS=48
N=10

for n in `seq 1 $N`; do
    for cores in `seq $CSTART $CSTEP $CEND`; do
	echo "spark-submit $JAR --offset 0 --input ~/Research/tmp/toprune_E110_T5.tsv --epsilon 110 --spatial $SPATIAL --partitions $PARTITIONS --cores $cores"
	spark-submit $JAR --offset 0 --input ~/Research/tmp/toprune_E110_T5.tsv --epsilon 110 --spatial $SPATIAL --partitions $PARTITIONS --cores $cores
    done
done
