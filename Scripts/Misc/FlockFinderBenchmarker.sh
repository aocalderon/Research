#!/bin/bash

DATASET_PATH="/home/acald013/Research/Datasets/Berlin/"
DATASET_NAME="berlin0-5"
DATASET_EXT=".tsv"
JAR="/home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.0.jar"


EPSILONS=( 50 60 70 80 90 100 )
DISTANCES=( 35 50 50 75 75 75 )
MU=3
DELTA=3
N=3

for n in `seq 1 $N`; do
    for((i=0;i<${#EPSILONS[@]};i++)); do
	spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]}
    done
done
