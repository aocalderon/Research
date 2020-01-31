#!/bin/bash

EPSILON=$1
MU=$2
MF_PARTITIONS=$3
D_PARTITIONS=$4

MF_ENTRIES=10
MF_LEVELS=8
MF_FRACTION=0.25
INPUT_PATH=/user/acald013/Datasets/LA/LA_25KTrajs/LA_25KTrajs_19.tsv
MASTER=yarn
CORES=4
EXECUTORS=36

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
    --class MF $CLASS_JAR \
    --input $INPUT_PATH \
    --epsilon $EPSILON --mu $MU \
    --mfpartitions $MF_PARTITIONS \
    --levels $MF_LEVELS --entries $MF_ENTRIES --fraction $MF_FRACTION \
    --dpartitions $D_PARTITIONS \
    --mfgrid

