#!/bin/bash

N=$1
DATASET="LA_25K.tsv"
WIDTH=800

for n in `seq $N`
do
    for EPSILON in `seq 4 2 12`
    do
        for MU in `seq 3 3`
        do
            echo "spark-submit --master spark://mr-hn:7077 --num-executors 10 --executor-cores 12 --class ICPE /home/acald013/Research/Scripts/Scala/ICPE/target/scala-2.11/icpe_2.11-0.1.jar --input ~/Research/Datasets/LA/${DATASET} --epsilon $EPSILON --mu $MU --width $WIDTH"
            spark-submit --master spark://mr-hn:7077 --num-executors 10 --executor-cores 12 --class ICPE /home/acald013/Research/Scripts/Scala/ICPE/target/scala-2.11/icpe_2.11-0.1.jar --input ~/Research/Datasets/LA/${DATASET} --epsilon $EPSILON --mu $MU --width $WIDTH

            echo "spark-submit --master spark://mr-hn:7077 --num-executors 10 --executor-cores 12 --class MF_QuadTree2 /home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.jar --input ~/Research/Datasets/LA/${DATASET} --epsilon $EPSILON --mu $MU --ffpartitions 16 --mfpartitions 100 --levels 32"
            spark-submit --master spark://mr-hn:7077 --num-executors 10 --executor-cores 12 --class MF_QuadTree2 /home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.jar --input ~/Research/Datasets/LA/${DATASET} --epsilon $EPSILON --mu $MU --ffpartitions 16 --mfpartitions 100 --levels 32
        done
    done
done
