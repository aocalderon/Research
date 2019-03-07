#!/bin/bash

DATASET_PATH="/home/acald013/Research/Datasets/Berlin/Quads/"
DATASET_NAME="Berlin_N3"
DATASET_EXT=".tsv"
JAR="/home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.0.jar"


EPSILONS=( 70 80 90 100 110)
DISTANCES=( 50 75 75 75 110)
MU=3
DELTA=3
N=5
CORES=( 1 2 4 8 )

for n in `seq 1 $N`; do
    for((c=0;c<${#CORES[@]};c++)); do
	for((i=0;i<${#EPSILONS[@]};i++)); do
	    echo "spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --master local[${CORES[c]}] --tag ${CORES[c]} --mfpartitions 625 --ffpartitions 625"
	    spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --master local[${CORES[c]}] --tag ${CORES[c]} --mfpartitions 625 --ffpartitions 625
	done
    done
done
