#!/bin/bash

##################################
# FF Multinode Scale up
##################################

DATASET_PATH="/home/ubuntu/Research/Datasets/Berlin/SideBySide/"
DATASET_PREFIX="B"
MF_PARTITIONS=60
FF_PARTITIONS=35
D_PARTITIONS=40
DATASET_EXT=".tsv"
JAR="/home/ubuntu/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.jar"
MASTER="driver"

EPSILONS=(  90 100 110 )
DISTANCES=( 75 75  100 )
MU=3
DELTA=3
N=1
DATASET_NAMES=( 1 2 3 )
EXECUTORS_SET=( 3 6 9 )
CORES=6
TIMESTAMP=4
PORTUI=4040

for n in `seq 1 $N`; do
    for((e=0;e<${#EXECUTORS_SET[@]};e++)); do
	DATASET_NAME="${DATASET_PREFIX}${DATASET_NAMES[e]}"
	yff=$FF_PARTITIONS
	xff=$((${DATASET_NAMES[e]} * FF_PARTITIONS))
	ymf=$MF_PARTITIONS
	xmf=$((${DATASET_NAMES[e]} * MF_PARTITIONS))
	yd=$D_PARTITIONS
	xd=$((${DATASET_NAMES[e]} * D_PARTITIONS))
	for((i=0;i<${#EPSILONS[@]};i++)); do
	    echo "spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --master $MASTER --cores $CORES --executors ${EXECUTORS_SET[e]} --spatial CUSTOM --mfcustomy $ymf --mfcustomx $xmf --ffcustomy $yff --ffcustomx $xff --dcustomy $yd --dcustomx $xd  --fftimestamp $TIMESTAMP --portui $PORTUI"
	    spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --master $MASTER --cores $CORES --executors ${EXECUTORS_SET[e]} --spatial CUSTOM --mfcustomy $ymf --mfcustomx $xmf --ffcustomy $yff --ffcustomx $xff --dcustomy $yd --dcustomx $xd  --fftimestamp $TIMESTAMP --portui $PORTUI
	done
    done
done
