#!/bin/bash

EPSILON=$1
MU=$2
DELTA=$3
FF_PARTITIONS=$4
MF_PARTITIONS=$5
MF_LEVELS=$6
MF_ENTRIES=$7
MF_FRACTION=$8

DISTANCE=100
MIN_INTERVAL=15
MAX_INTERVAL=20
INPUT_PATH=/user/acald013/Datasets/LA/LA_25KTrajs/
INPUT_TAG=LA_25KTrajs
MASTER=yarn
CORES=3
EXECUTORS=40

SPARK_JARS=/home/acald013/Spark/2.4/jars/
CLASS_JAR=/home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-0.1.jar
LOG_FILE=/home/acald013/Spark/2.4/conf/log4j.properties

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
    --epsilon $EPSILON --distance $DISTANCE --mu $MU --delta $DELTA \
    --ffpartitions $FF_PARTITIONS --mfpartitions $MF_PARTITIONS \
    --levels $MF_LEVELS --entries $MF_ENTRIES --fraction $MF_FRACTION \
    --stream --save \
    --mininterval $MIN_INTERVAL --maxinterval $MAX_INTERVAL 
