#!/bin/bash

IPATH=$1
OPATH=$2
EPSILON=20
MU=3


for INPUT in "$IPATH"/*.tsv
do
    echo "spark-submit --master local[11] --class MF_QuadTree2 /home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.jar --input $INPUT --epsilon $EPSILON --mu $MU --ffpartitions 4 --levels 2 --entries 100 --output $OPATH --mfdebug"
    spark-submit --master local[11] --class MF_QuadTree2 /home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.jar --input $INPUT --epsilon $EPSILON --mu $MU --ffpartitions 4 --levels 2 --entries 100 --output $OPATH --mfdebug
done
       
