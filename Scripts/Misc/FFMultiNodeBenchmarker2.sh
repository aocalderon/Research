#!/bin/bash

DATASET_PATH="/home/acald013/Research/Datasets/Berlin/Quads/"
DATASET_NAME="Berlin_N3"
DATASET_EXT=".tsv"
PARTITIONS_SET=( 2500 2500 2500 )
JAR="/home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.0.jar"

EPSILONS=( 90 100 ) 
DISTANCES=( 75 75 )
MU=3
DELTA=3
N=5
ESTART=1
EEND=3
CORES=4

for n in `seq 1 $N`; do
    for((i=0;i<${#EPSILONS[@]};i++)); do
	for executors in `seq $ESTART $EEND`; do
	    PARTITIONS=${PARTITIONS_SET[executors-1]}
	    echo "spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --cores $CORES --executors $executors --mfpartitions $PARTITIONS --ffpartitions $PARTITIONS"
	    spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --cores $CORES --executors $executors --mfpartitions $PARTITIONS --ffpartitions $PARTITIONS
	done
    done
done
