#!/bin/bash

MASTER="yarn"
EXECUTORS=12
CORES=9
DMEMORY=12g
EMEMORY=30g

EPSILON=10
PARTITIONS=1080
CAPACITY=200
DEBUG=""

while getopts "e:p:x:c:a:d" OPTION; do
    case $OPTION in
    e)
        EPSILON=$OPTARG
        ;;
    p)
        PARTITIONS=$OPTARG
        ;;
    x)
	EXECUTORS=$OPTARG
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
CLASS_NAME=edu.ucr.dblab.djoin.DisksFinder_Control
LOG_FILE=$HOME/Spark/2.4/conf/log4j.properties

HUSER=/user/acald013/
POINTS=hdfs://$HUSER/Trajs/LA/LA50K
#POINTS=hdfs://$HUSER/Trajs/Test/P10K

spark-submit \
    --conf spark.default.parallelism=108 \
    --files "$LOG_FILE" --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:"$LOG_FILE" \
    --jars "${SPARK_JARS}"geospark-1.2.0.jar,"${SPARK_JARS}"geospark-sql_2.3-1.2.0.jar,"${SPARK_JARS}"geospark-viz_2.3-1.2.0.jar,"${SPARK_JARS}"scallop_2.11-3.1.5.jar,"${SPARK_JARS}"spark-measure_2.11-0.16.jar,"${SPARK_JARS}"utils_2.11.jar \
    --num-executors $EXECUTORS --executor-cores $CORES --executor-memory $EMEMORY --driver-memory $DMEMORY \
    --master "$MASTER" --deploy-mode client \
    --class "$CLASS_NAME" "$CLASS_JAR" $DEBUG \
    --points "$POINTS" --epsilon "$EPSILON" \
    --partitions "$PARTITIONS" --capacity "$CAPACITY" --factor -1
