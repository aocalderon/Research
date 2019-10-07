#!/bin/bash

N=$1
DATASET="/home/acald013/Datasets/Brinkhoff/B0_1K/"
WIDTH=800
MU=3

for n in `seq $N`
do
    for EPSILON in `seq 5 5 15`
    do
        for T in `seq 1 100`
        do
            echo "spark-submit --master spark://mr-hn:7077 --num-executors 40 --executor-cores 3 --class ICPE /home/acald013/Research/Scripts/Scala/ICPE/target/scala-2.11/icpe_2.11-0.1.jar --input ${DATASET}B_${T}.tsv --epsilon $EPSILON --mu $MU --width $WIDTH"
            spark-submit --master spark://mr-hn:7077 --num-executors 40 --executor-cores 3 --class ICPE /home/acald013/Research/Scripts/Scala/ICPE/target/scala-2.11/icpe_2.11-0.1.jar --input ${DATASET}B_${T}.tsv --epsilon $EPSILON --mu $MU --width $WIDTH

            #echo "spark-submit --master spark://mr-hn:7077 --num-executors 40 --executor-cores 3 --class MF_QuadTree2 /home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.jar --input ${DATASET}B_${T}.tsv --epsilon $EPSILON --mu $MU --ffpartitions 16 --mfpartitions 100 --levels 32"
            #spark-submit --master spark://mr-hn:7077 --num-executors 40 --executor-cores 3 --class MF_QuadTree2 /home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.jar --input ${DATASET}B_${T}.tsv --epsilon $EPSILON --mu $MU --ffpartitions 16 --mfpartitions 100 --levels 32
        done
    done
done
