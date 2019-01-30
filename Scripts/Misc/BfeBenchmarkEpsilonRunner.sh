#!/bin/bash

DATASET="/home/acald013/Research/Datasets/Berlin/berlin0-5.tsv"
M=3
D=3
E=10

./BfeBenchmark.sh $DATASET 50 $M $D
./BfeBenchmark.sh $DATASET 60 $M $D
./BfeBenchmark.sh $DATASET 70 $M $D
./BfeBenchmark.sh $DATASET 80 $M $D
./BfeBenchmark.sh $DATASET 90 $M $D
./BfeBenchmark.sh $DATASET 100 $M $D

