#!/bin/bash

DATASET_PATH="/home/acald013/Datasets/LA/LA_25KTrajs/"
DATASET_NAME="LA_25KTrajs"
DATASET_EXT="tsv"
JAR="/home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-0.1.jar"
LISTENER="--conf spark.extraListeners=TaskSparkListener"
CORES=4
EXECUTORS=10
START=0
END=50
SPEED=100
EPSILON=( 5 10 15 25 )
MU=( 3 )
DELTA=( 3 )

for E in "${EPSILON[@]}"
do
    for M in "${MU[@]}"
    do
	for D in "${DELTA[@]}"
	do
	    echo "spark-submit $LISTENER --master spark://mr-hn:7077 --class FF $JAR --input_path $DATASET_PATH --input_tag $DATASET_NAME --epsilon $E --distance $SPEED --mu $M --delta $D --levels 8 --entries 200 --ffpartitions 10 --stream --mininterval $START --maxinterval $END --save --cores $CORES --executors $EXECUTORS"
	    spark-submit $LISTENER --master spark://mr-hn:7077 --class FF $JAR --input_path $DATASET_PATH --input_tag $DATASET_NAME --epsilon $E --distance $SPEED --mu $M --delta $D --levels 8 --entries 200 --ffpartitions 10 --stream --mininterval $START --maxinterval $END --save --cores $CORES --executors $EXECUTORS

	    echo "spark-submit $LISTENER --master spark://mr-hn:7077 --class FE $JAR --input $DATASET_PATH --tag $DATASET_NAME --i $START --n $END --epsilon $E --mu $M --delta $D --entries 20 --ffpartitions 8 --envelope --interval 10 --save --width 5000 --speed $SPEED --cores $CORES --executors $EXECUTORS"
	    spark-submit $LISTENER --master spark://mr-hn:7077 --num-executors 40 --executor-cores 3 --class FE $JAR --input $DATASET_PATH --tag $DATASET_NAME --i $START --n $END --epsilon $E --mu $M --delta $D --entries 20 --ffpartitions 8 --envelope --interval 10 --save --width 5000 --speed $SPEED --cores $CORES --executors $EXECUTORS
	    
	done
    done
done
			   
			  
