#!/bin/bash

EXECUTORS=12
CORES=9
DMEMORY=12g
EMEMORY=30g
SPARK_JARS=$HOME/Spark/2.4/jars/
CLASS_JAR=$HOME/Research/Scripts/Scala/DistanceJoin/target/scala-2.11/geotester_2.11-0.1.jar
CLASS_NAME="edu.ucr.dblab.djoin.QuadTreeTest"
LOG_FILE=$HOME/Spark/2.4/conf/log4j.properties

THE_MASTER="yarn"
THE_INPUT="file:///home/and/Datasets/Points_N50K_E40.tsv"
THE_PARTITIONS=1024
THE_EPSILON=10
THE_CAPACITY=50
THE_FRACTION=0.1
THE_LEVELS=6

while getopts "i:p:e:c:f:v:l" OPTION; do
    case $OPTION in
    i)
        THE_INPUT=$OPTARG
        ;;
    p)
        THE_PARTITIONS=$OPTARG
        ;;
    e)
        THE_EPSILON=$OPTARG
        ;;
    c)
        THE_CAPACITY=$OPTARG
        ;;
    f)
        THE_FRACTION=$OPTARG
        ;;
    v)
        THE_LEVELS=$OPTARG
        ;;
    l)
	THE_MASTER="local[*]"
	;;
    *)
        echo "Incorrect options provided"
        exit 1
        ;;
    esac
done

spark-submit \
    --files $LOG_FILE \
    --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$LOG_FILE \
    --jars ${SPARK_JARS}geospark-1.2.0.jar,${SPARK_JARS}geospark-sql_2.3-1.2.0.jar,${SPARK_JARS}geospark-viz_2.3-1.2.0.jar,${SPARK_JARS}scallop_2.11-3.1.5.jar \
    --master $THE_MASTER --deploy-mode client \
    --num-executors $EXECUTORS --executor-cores $CORES --executor-memory $EMEMORY --driver-memory $DMEMORY \
    --class $CLASS_NAME $CLASS_JAR --input $THE_INPUT --partitions $THE_PARTITIONS --epsilon $THE_EPSILON --capacity $THE_CAPACITY --fraction $THE_FRACTION --levels $THE_LEVELS
