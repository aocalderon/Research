#!/bin/bash

METHOD=$1
EPSILON=$2
PARTITIONS=${3:-1}
CORES=${4:-1}
CAPACITY=${5:-10}
FRACTION=${6:-0.025}
LEVELS=${7:-5}
LPARTS=${8:-10}
THRESHOLD=${9:-100000}

MU=3

SPARK_JARS=$HOME/Spark/2.4/jars/
CLASS_JAR=$HOME/Research/Scripts/Scala/DistanceJoin/target/scala-2.11/geotester_2.11-0.1.jar
CLASS_NAME=edu.ucr.dblab.djoin.DiskFinderTest
LOG_FILE=$HOME/Spark/2.4/conf/log4j.properties

MASTER="local[$CORES]"

#POINTS=file://$HOME/Datasets/Test/Points_N50K_E40.tsv
POINTS=file://$HOME/Research/Datasets/Test/Points_N10K_E10.tsv
#POINTS=file://$HOME/Research/Datasets/Test/Points_N1K_E20.tsv
#POINTS=file://$HOME/Research/Datasets/Test/Points_N20_E1.tsv

spark-submit \
    --files "$LOG_FILE" --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:"$LOG_FILE" \
    --jars "${SPARK_JARS}"geospark-1.2.0.jar,"${SPARK_JARS}"geospark-sql_2.3-1.2.0.jar,"${SPARK_JARS}"scallop_2.11-3.1.5.jar,"${SPARK_JARS}"utils_2.11.jar \
    --master "$MASTER" \
    --class "$CLASS_NAME" "$CLASS_JAR" \
    --points "$POINTS" \
    --partitions "$PARTITIONS" \
    --epsilon "$EPSILON" --mu "$MU" \
    --threshold "$THRESHOLD" \
    --method "$METHOD" --lparts "$LPARTS" \
    --capacity "$CAPACITY" --fraction "$FRACTION" --levels "$LEVELS"
