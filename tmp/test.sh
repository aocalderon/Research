#!/bin/bash

for EPSILON in `seq 20 2 40`
do
    for MU in `seq 5 5`
    do
        echo "spark-submit --class DBScanOnSpark /home/acald013/Research/Scripts/Scala/DBSCAN/target/scala-2.11/dbscan_2.11-0.1.jar --input ~/Research/Datasets/LA/LA_10K.tsv --epsilon $EPSILON --mu $MU --partitions 16"
        spark-submit --class DBScanOnSpark /home/acald013/Research/Scripts/Scala/DBSCAN/target/scala-2.11/dbscan_2.11-0.1.jar --input ~/Research/Datasets/LA/LA_10K.tsv --epsilon $EPSILON --mu $MU --partitions 16

        echo "spark-submit --class MF_QuadTree2 /home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.jar --input ~/Research/Datasets/LA/LA_10K.tsv --epsilon $EPSILON --mu $MU --master 169.235.27.138 --cores 4 --executors 3 --ffpartitions 16 --mfpartitions 100 --levels 32"
        spark-submit --class MF_QuadTree2 /home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.jar --input ~/Research/Datasets/LA/LA_10K.tsv --epsilon $EPSILON --mu $MU --master 169.235.27.138 --cores 4 --executors 3 --ffpartitions 16 --mfpartitions 100 --levels 32
    done
done
