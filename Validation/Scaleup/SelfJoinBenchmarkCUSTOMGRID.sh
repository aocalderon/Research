#!/bin/bash                                                                                                                                                                                     

DATASET_PATH="/home/acald013/Research/Validation/Scaleup/"
DATASET_PREFIX="SJ_B"
PARTITIONS=30
DATASET_EXT=".tsv"
JAR="/home/acald013/Research/Validation/Scaleup/target/scala-2.11/scaleup_2.11-0.1.jar"
MASTER="169.235.27.138"
CORES=4

N=5
GRID="CUSTOM"
DISTANCES=( 110 )
EXECUTORS_SET=( 3 1 )
DATASETS_SET=( 3 1 )
Y=50

for n in `seq 1 $N`; do
    for X in `seq 30 10 70`; do
        for((e=0;e<${#EXECUTORS_SET[@]};e++)); do
            DATASET_COUNT=${DATASETS_SET[e]}
            EXECUTORS=${EXECUTORS_SET[e]}
            echo "spark-submit --class SelfJoin $JAR --input ${DATASET_PATH}${DATASET_PREFIX}${DATASET_COUNT}.tsv --executors $EXECUTORS --grid $GRID --customx $X --customy $Y"
            spark-submit --class SelfJoin $JAR --input ${DATASET_PATH}${DATASET_PREFIX}${DATASET_COUNT}.tsv --executors $EXECUTORS --grid $GRID --customx $X --customy $Y
        done
    done
done



 
