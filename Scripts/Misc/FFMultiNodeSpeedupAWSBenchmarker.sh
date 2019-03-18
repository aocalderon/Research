#!/bin/bash                             

##################################
# FF Multinode Speed up
##################################

DATASET_PATH="/home/acald013/Research/Datasets/Berlin/SideBySide/"
DATASET_NAME="B4"
DATASET_EXT=".tsv"
PARTITIONS_YMF=30
PARTITIONS_XMF=30
PARTITIONS_Y=30
PARTITIONS_X=30
JAR="/home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.0.jar"
MASTER="spark://ip-172-31-4-82:7077"

EPSILONS=( 110 )
DISTANCES=(100 )
MU=3
DELTA=3
N=1
ESTART=1
EEND=4
CORES=7

for n in `seq 1 $N`; do
    for((i=0;i<${#EPSILONS[@]};i++)); do
        for executors in `seq $ESTART $EEND`; do
            CUSTOMXMF=$((PARTITIONS_XMF * executors))
            CUSTOMYMF=$PARTITIONS_YMF
            CUSTOMX=$((PARTITIONS_X * executors))
            CUSTOMY=$PARTITIONS_Y
            echo "spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} -\
-master $MASTER --cores $CORES --executors $executors --spatial CUSTOM --customy $CUSTOMY --customx $CUSTOMX --customymf $CUSTOMYMF --customxmf $CUSTOMXMF"
            spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --maste\
r $MASTER --cores $CORES --executors $executors --spatial CUSTOM --customy $CUSTOMY --customx $CUSTOMX --customymf $CUSTOMYMF --customxmf $CUSTOMXMF
        done
    done
done
