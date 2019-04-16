#!/bin/bash                             

##################################
# FF Multinode Speed up for Mu
##################################

DATASET_PATH="/home/acald013/Research/Datasets/Berlin/SideBySide/"
DATASET_NAME="B4"
DATASET_EXT=".tsv"
CUSTOMYMF=40
CUSTOMXMF=160
CUSTOMY=50
CUSTOMX=200
JAR="/home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.jar"
MASTER="169.235.27.138"
EXECUTORS_SET=( 1 2 3 )
CORES=4
N=3

TIMESTAMPS=5
MU_SET=( 3 5 7 9 )
DELTA=3
EPSILON=100
DISTANCE=75

for n in `seq 1 $N`; do
    for((i=0;i<${#EXECUTORS_SET[@]};i++)); do
        EXECUTORS=${EXECUTORS_SET[i]}
        for((m=0;m<${#MU_SET[@]};m++)); do
            echo "spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon $EPSILON --mu ${MU_SET[m]} --delta $DELTA --distance $DISTANCE --master $MASTER --cores $CORES --executors $EXECUTORS --spatial CUSTOM --customy $CUSTOMY --customx $CUSTOMX --customymf $CUSTOMYMF --customxmf $CUSTOMXMF --fftimestamp $TIMESTAMPS"
            spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon $EPSILON --mu ${MU_SET[m]} --delta $DELTA --distance $DISTANCE --master $MASTER --cores $CORES --executors $EXECUTORS --spatial CUSTOM --customy $CUSTOMY --customx $CUSTOMX --customymf $CUSTOMYMF --customxmf $CUSTOMXMF --fftimestamp $TIMESTAMPS
	    done
    done
done
