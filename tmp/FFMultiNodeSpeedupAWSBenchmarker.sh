#!/bin/bash                             

##################################
# FF Multinode Speed up
##################################

DATASET_PATH="/home/acald013/Research/Datasets/Berlin/SideBySide/"
DATASET_NAME="B4"
DATASET_EXT=".tsv"
CUSTOMYMF=40
CUSTOMXMF=160
CUSTOMY=50
CUSTOMX=200
JAR="/home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.0.jar"
MASTER="ip-172-31-11-47"

EPSILONS=( 90 100 110 )
DISTANCES=( 75 75 100 )
MU=3
DELTA=3
N=10
EXECUTORS_SET=( 8 )
CORES=4

for n in `seq 1 $N`; do
    for((i=0;i<${#EPSILONS[@]};i++)); do
        for((e=0;e<${#EXECUTORS_SET[@]};e++)); do
	    EXECUTORS=${EXECUTORS_SET[e]}
            echo "spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --master $MASTER --cores $CORES --executors $EXECUTORS --spatial CUSTOM --customy $CUSTOMY --customx $CUSTOMX --customymf $CUSTOMYMF --customxmf $CUSTOMXMF --dpartitions 2 --fftimestamp 4"
            spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --master $MASTER --cores $CORES --executors $EXECUTORS --spatial CUSTOM --customy $CUSTOMY --customx $CUSTOMX --customymf $CUSTOMYMF --customxmf $CUSTOMXMF --dpartitions 2 --fftimestamp 4
        done
    done
done
