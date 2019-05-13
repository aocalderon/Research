#!/bin/bash

##################################
# FF Multinode Scale up
##################################

DATASET_PATH="/home/acald013/Research/Datasets/Berlin/SideBySide/"
DATASET_PREFIX="B"
MF_PARTITIONS=30
FF_PARTITIONS=30
DATASET_EXT=".tsv"
JAR="/home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.jar"
MASTER="169.235.27.138"

EPSILONS=(  110 )
DISTANCES=( 100 )
MU=3
DELTA=3
N=1
EXECUTORS_SET=( 3 1 )
CORES=4
TIMESTAMP=5

for n in `seq 1 $N`; do
    for((e=0;e<${#EXECUTORS_SET[@]};e++)); do
	DATASET_NAME="${DATASET_PREFIX}${EXECUTORS_SET[e]}"
	yff=$FF_PARTITIONS
	xff=$((${EXECUTORS_SET[e]} * FF_PARTITIONS))
	ymf=$MF_PARTITIONS
	xmf=$((${EXECUTORS_SET[e]} * MF_PARTITIONS))
	for((i=0;i<${#EPSILONS[@]};i++)); do
	    echo "spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --master $MASTER --cores $CORES --executors ${EXECUTORS_SET[e]} --spatial CUSTOM --customymf $ymf --customxmf $xmf --customy $yff --customx $xff --fftimestamp $TIMESTAMP"
	    spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --master $MASTER --cores $CORES --executors ${EXECUTORS_SET[e]} --spatial CUSTOM --customymf $ymf --customxmf $xmf --customy $yff --customx $xff  --fftimestamp $TIMESTAMP
	done
    done
done
