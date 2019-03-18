#!/bin/bash

##################################
# FF Multinode Scale up
##################################

DATASET_PATH="/home/acald013/Research/Datasets/Berlin/SideBySide/"
DATASETS=( "B1" "B2" "B3" "B4" )
MF_PARTITIONS=30
FF_PARTITIONS=30
DATASET_EXT=".tsv"
JAR="/home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.0.jar"
MASTER="spark://ip-172-31-11-47:7077"

EPSILONS=(  110 )
DISTANCES=( 100 )
MU=3
DELTA=3
N=1
ESTART=1
EEND=4
CORES=7

for n in `seq 1 $N`; do
    for executors in `seq $ESTART $EEND`; do
	DATASET_NAME=${DATASETS[executors-1]}
	yff=$MF_PARTITIONS
	xff=$((executors * MF_PARTITIONS))
	ymf=$FF_PARTITIONS
	xmf=$((executors * FF_PARTITIONS))
	for((i=0;i<${#EPSILONS[@]};i++)); do
	    echo "spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --cores $CORES --executors $executors --spatial CUSTOM --customymf $ymf --customxmf $xmf --customy $yff --customx $xff"
	    spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --cores $CORES --executors $executors --spatial CUSTOM --customymf $ymf --customxmf $xmf --customy $yff --customx $xff  
	done
    done
done
