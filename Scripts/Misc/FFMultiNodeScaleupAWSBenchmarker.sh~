#!/bin/bash

DATASET_PATH="/home/acald013/Research/Datasets/Berlin/SideBySide/"
DATASETS=( "B1" "B2" "B3" )
#PARTITIONS_SET=( 16 64 64)
MF_PARTITIONS=29
FF_PARTITIONS=45
DATASET_EXT=".tsv"
JAR="/home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.0.jar"

EPSILONS=(  90 100 110 )
DISTANCES=( 75  75 100 )
MU=3
DELTA=3
N=5
ESTART=1
EEND=3
CORES=4

for n in `seq 1 $N`; do
    for executors in `seq $ESTART $EEND`; do
	DATASET_NAME=${DATASETS[executors-1]}
	#PARTITIONS=${PARTITIONS_SET[executors-1]}
	yff=$MF_PARTITIONS
	xff=$((executors * MF_PARTITIONS))
	ymf=$FF_PARTITIONS
	xmf=$((executors * FF_PARTITIONS))
	for((i=0;i<${#EPSILONS[@]};i++)); do
	    echo "spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --cores $CORES --executors $executors --spatial CUSTOM --customymf $ymf --customxmf $xmf --customy $yff --customx $xff"
	    spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --cores $CORES --executors $executors --spatial CUSTOM --customymf $ymf --customxmf $xmf --customy $yff --customx $xff  
	    #--mfpartitions $PARTITIONS --ffpartitions $PARTITIONS --spatial QUADTREE
	done
    done
done
