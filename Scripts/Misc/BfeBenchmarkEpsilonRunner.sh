#!/bin/bash

DATASET="/home/acald013/Research/Datasets/Berlin/berlin0-10.tsv"
MU=4
DELTA=5

./BfeBenchmark.sh $DATASET 40 $MU $DELTA
./BfeBenchmark.sh $DATASET 50 $MU $DELTA
./BfeBenchmark.sh $DATASET 60 $MU $DELTA
./BfeBenchmark.sh $DATASET 70 $MU $DELTA
./BfeBenchmark.sh $DATASET 80 $MU $DELTA
./BfeBenchmark.sh $DATASET 90 $MU $DELTA
./BfeBenchmark.sh $DATASET 100 $MU $DELTA
./BfeBenchmark.sh $DATASET 150 $MU $DELTA
./BfeBenchmark.sh $DATASET 200 $MU $DELTA
