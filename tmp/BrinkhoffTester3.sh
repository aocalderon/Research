#!/bin/bash

N=$1
DATASET="/home/acald013/Datasets/Brinkhoff/BO_0-1K/"
WIDTH=300

for n in `seq $N`
do
    for EPSILON in 3.77 7.55 11.33
    do
        for T in `seq 1 100`
        do
            echo "spark-submit --master local[1] --class ICPETester /home/acald013/Research/Scripts/Scala/ICPE/target/scala-2.11/icpe_2.11-0.1.jar --input ${DATASET}B_${T}.tsv --epsilon $EPSILON --width $WIDTH"
            spark-submit --master local[1] --class ICPETester /home/acald013/Research/Scripts/Scala/ICPE/target/scala-2.11/icpe_2.11-0.1.jar --input ${DATASET}B_${T}.tsv --epsilon $EPSILON --width $WIDTH
        done
    done
done

# --master spark://mr-hn:7077 --num-executors 40 --executor-cores 3 
