#!/bin/bash

EPSILON=$1
PARTITIONS=$2
DPARTITIONS=$3
GRIDTYPE=$4
INDEXTYPE=$5

SPARK_JARS=/home/acald013/Spark/2.4/jars/
CLASS_JAR=/home/acald013/Research/Scripts/Scala/Geo/target/scala-2.11/geotester_2.11-0.1.jar
CLASS_NAME=edu.ucr.dblab.GeoTesterDF
LISTENER=spark.extraListeners=TaskSparkListener
LOG_FILE=/home/acald013/Spark/2.4/conf/log4j.properties

MASTER=yarn
EXECUTORS=36
CORES=3
DMEMORY=4g
EMEMORY=4g

#DATASET=/user/acald013/Datasets/LA/LA_25KTrajs/LA_25KTrajs_19.tsv
DATASET=file:///tmp/LA_25KTrajs_19.tsv

spark-submit --conf spark.default.parallelism=1024 \
    --conf spark.locality.wait=0s \
    --conf spark.locality.wait.node=0s \
    --conf spark.locality.wait.rack=3s \
    --files $LOG_FILE \
    --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$LOG_FILE \
    --jars ${SPARK_JARS}geospark-1.2.0.jar,${SPARK_JARS}geospark-sql_2.3-1.2.0.jar,${SPARK_JARS}scallop_2.11-3.1.5.jar,${SPARK_JARS}utils_2.11.jar \
    --master $MASTER --deploy-mode client \
    --num-executors $EXECUTORS --executor-cores $CORES --executor-memory $EMEMORY --driver-memory $DMEMORY \
    --class $CLASS_NAME $CLASS_JAR \
    --input $DATASET \
    --epsilon $EPSILON --partitions $PARTITIONS --dpartitions $DPARTITIONS \
    --gridtype $GRIDTYPE --indextype $INDEXTYPE

#    --conf $LISTENER \
