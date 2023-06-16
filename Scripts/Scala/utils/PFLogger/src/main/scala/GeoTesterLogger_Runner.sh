#!/bin/bash

SPARK_JARS=/home/acald013/Spark/2.4/jars/
LOG_FILE=/home/acald013/Spark/2.4/conf/log4j.properties
JAR_FILE=/home/acald013/Research/utils/PFLogger/target/scala-2.11/pflogger_2.11-0.1.jar

spark-submit \
    --files $LOG_FILE \
    --jars ${SPARK_JARS}scallop_2.11-3.1.5.jar \
    --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$LOG_FILE \
    --master local[10] \
    --class org.dblab.GeoTesterLogger $JAR_FILE \
    --input /user/acald013/Datasets/Logs/GeoTesterRDD.txt \
    --output ~/Research/tmp/GeoTesterRDD.txt
