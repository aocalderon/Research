#!/bin/bash

JAR="/home/acald013/Research/Scripts/Scala/PLCM/target/scala-2.11/plcm_2.11-0.1.jar"


PARTITIONS=( 10 20 30 40 50 60 70 )
N=1

for n in `seq 1 $N`; do
    for((i=0;i<${#PARTITIONS[@]};i++)); do
	echo "spark-submit $JAR --offset 0 --input ~/Research/tmp/toprune_5.tsv --epsilon 100 --spatial QUADTREE --partitions ${PARTITIONS[i]}"
	spark-submit $JAR --offset 0 --input ~/Research/tmp/toprune_5.tsv --epsilon 100 --spatial KDBTREE --partitions ${PARTITIONS[i]}
    done
done
