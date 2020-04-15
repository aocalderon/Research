#!/bin/bash

EPSILON=$1
MU=3
INDEXTYPE="quadtree"
GRIDTYPE="quadtree"
CAPACITY=$2

SPARK_JARS=/home/and/Spark/2.4/jars/
CLASS_JAR=/home/and/Research/Scripts/Scala/Geo/target/scala-2.11/geotester_2.11-0.1.jar
CLASS_NAME=GeoTesterRDD_Viz
LISTENER=spark.extraListeners=TaskSparkListener
LOG_FILE=/home/and/Spark/2.4/conf/log4j.properties

MASTER=local[4]
EXECUTORS=12
CORES=9
DMEMORY=10g
EMEMORY=30g
PARTITIONS=4
PARALLELISM=4

#DATASET=/user/acald013/Datasets/LA/LA_50KTrajs/LA_50K_320.tsv
DATASET=/home/and/Datasets/data.tsv

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
    --gridtype $GRIDTYPE --indextype $INDEXTYPE --capacity $CAPACITY --debug
#    --conf $LISTENER \
