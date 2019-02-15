#!/bin/bash

JAR="/home/acald013/Research/Scripts/Scala/PLCM/target/scala-2.11/plcm_2.11-0.1.jar"


PARTITIONS=( 9 10 11 12 13 14 15 16 17 18 19 20 )
N=10

for n in `seq 1 $N`; do
    for((i=0;i<${#PARTITIONS[@]};i++)); do
	echo "spark-submit $JAR --offset 0 --input ~/Research/tmp/toprune_5.tsv --epsilon 100 --spatial QUADTREE --partitions ${PARTITIONS[i]}"
	spark-submit $JAR --offset 0 --input ~/Research/tmp/toprune_5.tsv --epsilon 100 --spatial QUADTREE --partitions ${PARTITIONS[i]}
    done
done
