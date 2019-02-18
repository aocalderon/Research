#!/bin/bash

DATASET_PATH="/home/acald013/Research/Datasets/Berlin/"
DATASET_NAME="berlin_N20K_T0-5"
DATASET_EXT=".tsv"
JAR="/home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.0.jar"


EPSILONS=(  90 100 110 ) 
DISTANCES=( 75 75 100 )
MU=3
DELTA=3
N=10
CSTART=3
CEND=12
CSTEP=3

for n in `seq 1 $N`; do
    for((i=0;i<${#EPSILONS[@]};i++)); do
	for cores in `seq $CSTART $CSTEP $CEND`; do	
	    echo "spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --cores $cores"
	    spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --cores $cores
	done
    done
done
