#!/bin/bash

METHOD=$1
EPSILON=$2
PARTITIONS=$3
CAPACITY=$4
FRACTION=$5
LEVELS=$6
MU=3

SPARK_JARS=$HOME/Spark/2.4/jars/
CLASS_JAR=$HOME/Research/Scripts/Scala/DistanceJoin/target/scala-2.11/geotester_2.11-0.1.jar
CLASS_NAME=edu.ucr.dblab.djoin.DistanceJoinTest
LOG_FILE=$HOME/Spark/2.4/conf/log4j.properties

MASTER=local[8]

POINTS=file://$HOME/Datasets/Test/Points_N50K_E40.tsv
CENTERS=file://$HOME/Datasets/Test/Centers_N50K_E40.tsv
#POINTS=file://$HOME/Research/Datasets/Test/Points_N10K_E10.tsv
#CENTERS=file://$HOME/Research/Datasets/Test/Centers_N10K_E10.tsv
#POINTS=file://$HOME/Research/Datasets/Test/Points_N1K_E20.tsv
#CENTERS=file://$HOME/Research/Datasets/Test/Centers_N1K_E20.tsv
#POINTS=file://$HOME/Research/Datasets/Test/Points_N20_E1.tsv
#CENTERS=file://$HOME/Research/Datasets/Test/Centers_N20_E1.tsv

spark-submit \
    --jars ${SPARK_JARS}geospark-1.2.0.jar,${SPARK_JARS}geospark-sql_2.3-1.2.0.jar,${SPARK_JARS}scallop_2.11-3.1.5.jar,${SPARK_JARS}utils_2.11.jar \
    --files $LOG_FILE --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:${LOG_FILE} \
    --master $MASTER \
    --class $CLASS_NAME $CLASS_JAR \
    --points $POINTS --centers $CENTERS \
    --partitions $PARTITIONS \
    --epsilon $EPSILON --mu $MU \
    --capacity $CAPACITY --fraction $FRACTION --levels $LEVELS \
    --method $METHOD --debug

