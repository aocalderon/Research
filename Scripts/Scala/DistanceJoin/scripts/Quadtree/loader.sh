#!/bin/bash

EXECUTORS=12
CORES=9
DMEMORY=12g
EMEMORY=30g
SPARK_JARS=$HOME/Spark/2.4/jars/
CLASS_JAR=$HOME/Research/Scripts/Scala/DistanceJoin/target/scala-2.11/geotester_2.11-0.1.jar
CLASS_NAME="edu.ucr.dblab.djoin.Loader"
LOG_FILE=$HOME/Spark/2.4/conf/log4j.properties

THE_MASTER="yarn"
THE_INPUT="hdfs:///user/acald013/tmp/LA50K.tsv"
THE_OUTPUT="hdfs:///user/acald013/tmp/trajs"
THE_PARTITIONS=1080
THE_HEADER=""
THE_DELIMITER="\t"

while getopts "i:o:p:d:h" OPTION; do
    case $OPTION in
    i)
        THE_INPUT=$OPTARG
        ;;
    o)
        THE_OUTPUT=$OPTARG
        ;;
    p)
        THE_PARTITIONS=$OPTARG
        ;;
    d)
	THE_DELIMITER=$OPTARG
	;;
    h)
	THE_HEADER="--header"
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
    --class $CLASS_NAME $CLASS_JAR --input $THE_INPUT --partitions $THE_PARTITIONS --output $THE_OUTPUT --delimiter $THE_DELIMITER --header $THE_HEADER
