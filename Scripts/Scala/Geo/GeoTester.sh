#!/bin/bash

EPSILON=$1
PARTITIONS=$2
GRIDTYPE=$3
INDEXTYPE=$4

SPARK_JARS=/home/acald013/Spark/2.4/jars/
CLASS_JAR=/home/acald013/Research/Scripts/Scala/Geo/target/scala-2.11/geotester_2.11-0.1.jar
CLASS_NAME=edu.ucr.dblab.GeoTester
LISTENER=spark.extraListeners=TaskSparkListener
LOG_FILE=/home/acald013/Spark/2.4/conf/log4j.properties
MASTER=local[10]

DATASET=/user/acald013/Datasets/LA/LA_25KTrajs/LA_25KTrajs_19.tsv

spark-submit \
    --files $LOG_FILE \
    --jars ${SPARK_JARS}geospark-1.2.0.jar,${SPARK_JARS}geospark-sql_2.3-1.2.0.jar,${SPARK_JARS}scallop_2.11-3.1.5.jar,${SPARK_JARS}utils_2.11.jar \
    --conf $LISTENER \
    --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$LOG_FILE \
    --master $MASTER \
    --class $CLASS_NAME $CLASS_JAR \
    --input $DATASET \
    --epsilon $EPSILON --partitions $PARTITIONS --gridtype $GRIDTYPE --indextype $INDEXTYPE
