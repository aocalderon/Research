#!/bin/bash

##################################
# FF Multinode Scale up for Delta
##################################

DATASET_PATH="/home/acald013/Research/Datasets/Berlin/SideBySide/"
DATASETS=( "B1" "B2" "B3" "B4" )
MF_PARTITIONS=30
FF_PARTITIONS=30
DATASET_EXT=".tsv"
JAR="/home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.jar"
MASTER="169.235.27.138"
EXECUTORS_SET=( 1 2 3)
CORES=4
N=3

TIMESTAMPS=6
MU=3
DELTA_SET=(3 4 5 6)
EPSILON=100
DISTANCE=75

for n in `seq 1 $N`; do
    for((i=0;i<${#EXECUTORS_SET[@]};i++)); do
        EXECUTORS=${EXECUTORS_SET[i]}
        DATASET_NAME=${DATASETS[EXECUTORS-1]}
        yff=$MF_PARTITIONS
        xff=$((EXECUTORS * MF_PARTITIONS))
        ymf=$FF_PARTITIONS
        xmf=$((EXECUTORS * FF_PARTITIONS))
        for((d=0;d<${#DELTA_SET[@]};d++)); do
            echo "spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon $EPSILON --mu $MU --delta ${DELTA_SET[d]} --distance $DISTANCE --master $MASTER --cores $CORES --executors $EXECUTORS --spatial CUSTOM --customymf $ymf --customxmf $xmf --customy $yff --customx $xff --fftimestamp $TIMESTAMPS"
            spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon $EPSILON --mu $MU --delta ${DELTA_SET[d]} --distance $DISTANCE --master $MASTER --cores $CORES --executors $EXECUTORS --spatial CUSTOM --customymf $ymf --customxmf $xmf --customy $yff --customx $xff --fftimestamp $TIMESTAMPS
	    done
    done
done
