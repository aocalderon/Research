#!/bin/bash

DATASET_PATH="/home/acald013/Research/Datasets/Berlin/"
DATASET_NAME="berlin_N20K_T0-5"
DATASET_EXT=".tsv"
JAR="/home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.0.jar"


EPSILONS=( 50 60 70 80 90 100 )
DISTANCES=( 35 50 50 75 75 75 )
MU=3
DELTA=3
N=5
CORES=( 1 2 4 8 )

for n in `seq 1 $N`; do
    for((c=0;c<${#CORES[@]};c++)); do
	for((i=0;i<${#EPSILONS[@]};i++)); do
	    echo "spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --master local[${CORES[c]}] --tag ${CORES[c]}"
	    spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --master local[${CORES[c]}] --tag ${CORES[c]}
	done
    done
done
