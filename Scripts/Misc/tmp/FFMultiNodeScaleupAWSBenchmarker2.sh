#!/bin/bash

##################################
# FF Multinode Scale up
##################################

DATASET_PATH="/home/ubuntu/Research/Datasets/Berlin/SideBySide/"
DATASET_PREFIX="B"
MF_PARTITIONS=30
FF_PARTITIONS=30
D_PARTITIONS=30
DATASET_EXT=".tsv"
JAR="/home/ubuntu/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.jar"
MASTER="driver"

EPSILONS=(  80 90 100 110 120 )
DISTANCES=( 75 75 75  100 100 )
MU=3
DELTA=3
N=2
EXECUTORS_SET=( 15 10 5 )
DATASETS_SET=( 3 2 1 )
CORES=4
TIMESTAMP=4
PORTUI=4040

for n in `seq 1 $N`; do
    for((e=0;e<${#EXECUTORS_SET[@]};e++)); do
	DATASET_NAME="${DATASET_PREFIX}${DATASETS_SET[e]}"
	yff=$FF_PARTITIONS
	xff=$((${DATASETS_SET[e]} * FF_PARTITIONS))
	ymf=$MF_PARTITIONS
	xmf=$((${DATASETS_SET[e]} * MF_PARTITIONS))
	 yd=$D_PARTITIONS
	 xd=$((${DATASETS_SET[e]} *  D_PARTITIONS))
	for((i=0;i<${#EPSILONS[@]};i++)); do
	    echo "spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --master $MASTER --cores $CORES --executors ${EXECUTORS_SET[e]} --spatial CUSTOM --mfcustomy $ymf --mfcustomx $xmf --ffcustomy $yff --ffcustomx $xff --dcustomy $yd --dcustomx $xd --fftimestamp $TIMESTAMP --portui $PORTUI"
	    #spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --master $MASTER --cores $CORES --executors ${EXECUTORS_SET[e]} --spatial CUSTOM --mfcustomy $ymf --mfcustomx $xmf --ffcustomy $yff --ffcustomx $xff --dcustomy $yd --dcustomx $xd --fftimestamp $TIMESTAMP --portui $PORTUI
	done
    done
done
