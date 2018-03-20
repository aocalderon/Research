#!/bin/bash

JAR_FILE="/home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar"
PARTITIONS=28
CORES=28
DATASET_PATH="Datasets/Berlin/"
DATASET="berlin0"
SPEED=10

ESTART=20
EEND=60
ESTEP=10
MSTART=3
MEND=3
MSTEP=1
DSTART=7
DEND=2
DSTEP=-1

for N in `seq 1 2`
do
  for E in `seq $ESTART $ESTEP $EEND` 
  do
    for M in `seq $MSTART $MSTEP $MEND` 
    do
      for D in `seq $DSTART $DSTEP $DEND` 
      do
        DELTA=`expr $D + 1`
        spark-submit --class FlockFinderBenchmark $JAR_FILE --partitions $PARTITIONS --cores $CORES --speed $SPEED --path $DATASET_PATH --dataset $DATASET-$D --epsilon $E --mu $M --delta $DELTA
      done
    done
  done
done

echo "DONE!!!"
