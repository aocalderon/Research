#!/bin/bash

##################################
# FF Multinode Scale up
##################################

DATASET_PATH="/home/acald013/Research/Datasets/Berlin/SideBySide/"
DATASETS=( "B1" "B2" "B3" "B4" )
MF_PARTITIONS=34
FF_PARTITIONS=40
DATASET_EXT=".tsv"
JAR="/home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.0.jar"
MASTER="ip-172-31-11-47"

EPSILONS=( 90 100 110 )
DISTANCES=( 75 75 100 )
MU=3
DELTA=3
N=10
EXECUTORS_SET=( 2 )
CORES=4

for n in `seq 1 $N`; do
    for((e=0;e<${#EXECUTORS_SET[@]};e++)); do
	EXECUTORS=${EXECUTORS_SET[e]}
	DATASET_NAME=${DATASETS[executors-1]}
	yff=$MF_PARTITIONS
	xff=$((EXECUTORS * MF_PARTITIONS))
	ymf=$FF_PARTITIONS
	xmf=$((EXECUTORS * FF_PARTITIONS))
	for((i=0;i<${#EPSILONS[@]};i++)); do
	    echo "spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --master $MASTER --cores $CORES --executors $EXECUTORS --spatial CUSTOM --customymf $ymf --customxmf $xmf --customy $yff --customx $xff"
	    spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --master $MASTER --cores $CORES --executors $EXECUTORS --spatial CUSTOM --customymf $ymf --customxmf $xmf --customy $yff --customx $xff  
	done
    done
done