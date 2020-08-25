#!/bin/bash

INPUT=$1
OUTPUT=$2
PARTITIONS=$3

spark-submit \
    --master local[*] \
    --jars /home/acald013/Spark/2.4/jars/scallop_2.11-3.1.5.jar \
    --class edu.ucr.dblab.djoin.Loader ~/Research/Scripts/Scala/DistanceJoin/target/scala-2.11/geotester_2.11-0.1.jar \
    --input $1 \
    --output $2 \
    --partitions $3
