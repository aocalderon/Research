#!/bin/bash

DATASET_PATH="/home/acald013/Research/Datasets/Berlin/"
DATASETS=( "berlin_N20K_T0-5" "berlin_N40K_T0-5" "berlin_N60K_T0-5" "berlin_N80K_T0-5" )
DATASET_EXT=".tsv"
JAR="/home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.0.jar"


EPSILONS=(  90 100 110 )
DISTANCES=( 75 75 100 )
MU=3
DELTA=3
N=1
NODES=4

for n in `seq 1 $N`; do
    for node in `seq 1 $NODES`; do
	echo "python /home/acald013/Research/Scripts/Python/NodesSetter.py -n $node"
	python /home/acald013/Research/Scripts/Python/NodesSetter.py -n $node
	
	DATASET_NAME=${DATASETS[node-1]}
	for((i=0;i<${#EPSILONS[@]};i++)); do
	    echo "spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --tag $node"
	    spark-submit --class FF $JAR --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon ${EPSILONS[i]} --mu $MU --delta $DELTA --distance ${DISTANCES[i]} --tag $node
	done
    done
done
