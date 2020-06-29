#!/bin/bash

METHOD="Partition"
EPSILON=10
PARTITIONS=1
CORES=1
CAPACITY=250
LEVELS=5
DEBUG=""

while getopts "m:e:p:c:a:f:l:n:t:u:d" OPTION; do
    case $OPTION in
    m)
        METHOD=$OPTARG
        ;;
    e)
        EPSILON=$OPTARG
        ;;
    p)
        PARTITIONS=$OPTARG
        ;;
    c)
	CORES=$OPTARG
	;;
    a)
	CAPACITY=$OPTARG
	;;
    d)
	DEBUG="--debug"
	;;
    *)
        echo "Incorrect options provided"
        exit 1
        ;;
    esac
done

SPARK_JARS=$HOME/Spark/2.4/jars/
CLASS_JAR=$HOME/Research/Scripts/Scala/DistanceJoin/target/scala-2.11/geotester_2.11-0.1.jar
CLASS_NAME=edu.ucr.dblab.djoin.DisksFinder
LOG_FILE=$HOME/Spark/2.4/conf/log4j.properties

MASTER="local[$CORES]"

#POINTS=file://$HOME/Datasets/Test/Points_N50K_E40.tsv
#POINTS=file://$HOME/Research/Datasets/Test/Points_N10K_E10.tsv
#POINTS=file://$HOME/Research/Datasets/Test/Points_N1K_E20.tsv
POINTS=file://$HOME/Research/Datasets/Test/Points_N20_E1.tsv

spark-submit \
    --files "$LOG_FILE" --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:"$LOG_FILE" \
    --jars "${SPARK_JARS}"geospark-1.2.0.jar,"${SPARK_JARS}"geospark-sql_2.3-1.2.0.jar,"${SPARK_JARS}"scallop_2.11-3.1.5.jar,"${SPARK_JARS}"utils_2.11.jar \
    --master "$MASTER" \
    --class "$CLASS_NAME" "$CLASS_JAR" $DEBUG \
    --points "$POINTS"  --method "$METHOD" --epsilon "$EPSILON" \
    --partitions "$PARTITIONS" --capacity "$CAPACITY"  
