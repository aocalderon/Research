#!/bin/bash

DATASET_PATH="/home/acald013/Research/Datasets/Berlin/Quads/"
DATASETS=( "Berlin_N1" "Berlin_N2" "Berlin_N3" )
PARTITIONS=( 625 2500 2500 )
DATASET_EXT=".tsv"
JAR="/home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.0.jar"


EPSILONS=(  110 )
DISTANCES=( 100 )
MU=3
DELTA=3
N=10
ESTART=1
EEND=3
CORES=4

for n in `seq 1 $N`; do
    for executors in `seq $ESTART $EEND`; do
	DATASET_NAME=${DATASETS[executors-1]}
	PARTITIONS=${PARTITIONS[executors-1]}
	for((i=0;i<${#EPSILONS[@]};i++)); do
	    echo "spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --cores $CORES --executors $executors --mfpartitions $PARTITIONS --ffpartitions $PARTITIONS"
	    spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --cores $CORES --executors $executors --mfpartitions $PARTITIONS --ffpartitions $PARTITIONS
	done
    done
done
