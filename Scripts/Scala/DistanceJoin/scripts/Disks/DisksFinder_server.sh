#!/bin/bash

METHOD="Partition"
EPSILON=10
PARTITIONS=1
EXECUTORS=12
CORES=8
DMEMORY=12g
EMEMORY=30g
CAPACITY=250
LEVELS=5
DEBUG=""

while getopts "m:e:p:x:c:a:f:l:n:t:u:d" OPTION; do
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
CLASS_NAME=edu.ucr.dblab.djoin.DisksFinder
LOG_FILE=$HOME/Spark/2.4/conf/log4j.properties

MASTER="yarn"

HUSER=/user/acald013/
POINTS=hdfs://$HUSER/Datasets/LA/LA_50KTrajs
#POINTS=file://$HOME/Research/Datasets/Test/Points_N10K_E10.tsv
#POINTS=file://$HOME/Research/Datasets/Test/Points_N1K_E20.tsv
#POINTS=file://$HOME/Research/Datasets/Test/Points_N20_E1.tsv

#    --conf spark.locality.wait=15s \
#    --conf spark.locality.wait.node=1s \
#    --conf spark.locality.wait.process=1s \
#    --conf spark.locality.wait.rack=1s \
spark-submit \
    --conf spark.default.parallelism=${PARTITIONS} \
    --files "$LOG_FILE" --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:"$LOG_FILE" \
    --jars "${SPARK_JARS}"geospark-1.2.0.jar,"${SPARK_JARS}"geospark-sql_2.3-1.2.0.jar,"${SPARK_JARS}"scallop_2.11-3.1.5.jar,"${SPARK_JARS}"spark-measure_2.11-0.16.jar,"${SPARK_JARS}"utils_2.11.jar \
    --num-executors $EXECUTORS --executor-cores $CORES --executor-memory $EMEMORY --driver-memory $DMEMORY \
    --master "$MASTER" --deploy-mode client \
    --class "$CLASS_NAME" "$CLASS_JAR" $DEBUG \
    --points "$POINTS"  --method "$METHOD" --epsilon "$EPSILON" \
    --partitions "$PARTITIONS" --capacity "$CAPACITY"  
