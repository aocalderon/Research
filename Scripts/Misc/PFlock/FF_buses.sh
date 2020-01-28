#!/bin/bash

MIN_INTERVAL=$1
MAX_INTERVAL=$2
INPUT_PATH=/user/acald013/Datasets/Buses/
INPUT_TAG=buses

SPARK_JARS=/home/acald013/Spark/2.4/jars/
CLASS_JAR=/home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-0.1.jar
LOG_FILE=/home/acald013/Spark/2.4/conf/log4j.properties
MASTER=yarn
CORES=4
EXECUTORS=12

spark-submit \
    --files $LOG_FILE \
    --jars ${SPARK_JARS}geospark-1.2.0.jar,${SPARK_JARS}scallop_2.11-3.1.5.jar \
    --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$LOG_FILE \
    --master $MASTER \
    --deploy-mode client \
    --num-executors $EXECUTORS \
    --executor-cores $CORES \
    --driver-memory 16g \
    --executor-memory 12g \
    --class FF $CLASS_JAR \
    --input_path $INPUT_PATH --input_tag $INPUT_TAG \
    --epsilon 1 --distance 10 --mu 3 --delta 3 \
    --levels 8 --entries 200 --fraction 0.1 \
    --ffpartitions 4 \
    --stream --save \
    --mininterval $MIN_INTERVAL --maxinterval $MAX_INTERVAL 
