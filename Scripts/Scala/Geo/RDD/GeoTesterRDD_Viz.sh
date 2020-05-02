#!/bin/bash

EPSILON=$1
CAPACITY=$2
FRACTION=$3
LEVELS=$4
MU=3
INDEXTYPE="quadtree"
GRIDTYPE="quadtree"

SPARK_JARS=$HOME/Spark/2.4/jars/
CLASS_JAR=$HOME/Research/Scripts/Scala/Geo/target/scala-2.11/geotester_2.11-0.1.jar
CLASS_NAME=GeoTesterRDD_Viz
LISTENER=spark.extraListeners=TaskSparkListener
LOG_FILE=$HOME/Spark/2.4/conf/log4j.properties

MASTER=yarn
EXECUTORS=12
CORES=9
DMEMORY=10g
EMEMORY=30g
PARTITIONS=256
PARALLELISM=256

DATASET=/user/acald013/Datasets/LA/LA_50KTrajs/LA_50K_320.tsv
#DATASET=/user/acald013/Datasets/Demo/data.tsv

spark-submit --conf spark.default.parallelism=${PARALLELISM} \
    --conf spark.driver.maxResultSize=4g \
    --conf spark.locality.wait=0s \
    --conf spark.locality.wait.node=0s \
    --conf spark.locality.wait.rack=3s \
    --files $LOG_FILE \
    --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:${LOG_FILE} \
    --jars ${SPARK_JARS}geospark-1.2.0.jar,${SPARK_JARS}geospark-sql_2.3-1.2.0.jar,${SPARK_JARS}scallop_2.11-3.1.5.jar,${SPARK_JARS}utils_2.11.jar \
    --master $MASTER --deploy-mode client \
    --num-executors $EXECUTORS --executor-cores $CORES --executor-memory $EMEMORY --driver-memory $DMEMORY \
    --class $CLASS_NAME $CLASS_JAR \
    --input $DATASET \
    --epsilon $EPSILON --mu $MU --partitions $PARTITIONS --parallelism $PARALLELISM \
    --gridtype $GRIDTYPE --indextype $INDEXTYPE \
    --capacity $CAPACITY --fraction $FRACTION --levels $LEVELS \
    --debug
