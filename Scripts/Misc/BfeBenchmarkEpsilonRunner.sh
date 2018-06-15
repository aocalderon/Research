#!/bin/bash

DATASET="/home/acald013/Research/Datasets/Berlin/berlin0-10.tsv"
M=5
D=6
E=100

./BfeBenchmark.sh $DATASET 80 $M $D
./BfeBenchmark.sh $DATASET 90 $M $D
./BfeBenchmark.sh $DATASET 100 $M $D
./BfeBenchmark.sh $DATASET 110 $M $D
./BfeBenchmark.sh $DATASET 120 $M $D

./BfeBenchmark.sh $DATASET $E 4 $D
./BfeBenchmark.sh $DATASET $E 5 $D
./BfeBenchmark.sh $DATASET $E 6 $D
./BfeBenchmark.sh $DATASET $E 7 $D
./BfeBenchmark.sh $DATASET $E 8 $D

./BfeBenchmark.sh $DATASET $E $M 3
./BfeBenchmark.sh $DATASET $E $M 4 
./BfeBenchmark.sh $DATASET $E $M 5
./BfeBenchmark.sh $DATASET $E $M 6
./BfeBenchmark.sh $DATASET $E $M 7

./BfeBenchmark.sh $DATASET 80 $M $D
./BfeBenchmark.sh $DATASET 90 $M $D
./BfeBenchmark.sh $DATASET 100 $M $D
./BfeBenchmark.sh $DATASET 110 $M $D
./BfeBenchmark.sh $DATASET 120 $M $D

./BfeBenchmark.sh $DATASET $E 4 $D
./BfeBenchmark.sh $DATASET $E 5 $D
./BfeBenchmark.sh $DATASET $E 6 $D
./BfeBenchmark.sh $DATASET $E 7 $D
./BfeBenchmark.sh $DATASET $E 8 $D

./BfeBenchmark.sh $DATASET $E $M 3
./BfeBenchmark.sh $DATASET $E $M 4 
./BfeBenchmark.sh $DATASET $E $M 5
./BfeBenchmark.sh $DATASET $E $M 6
./BfeBenchmark.sh $DATASET $E $M 7

./BfeBenchmark.sh $DATASET 80 $M $D
./BfeBenchmark.sh $DATASET 90 $M $D
./BfeBenchmark.sh $DATASET 100 $M $D
./BfeBenchmark.sh $DATASET 110 $M $D
./BfeBenchmark.sh $DATASET 120 $M $D

./BfeBenchmark.sh $DATASET $E 4 $D
./BfeBenchmark.sh $DATASET $E 5 $D
./BfeBenchmark.sh $DATASET $E 6 $D
./BfeBenchmark.sh $DATASET $E 7 $D
./BfeBenchmark.sh $DATASET $E 8 $D

./BfeBenchmark.sh $DATASET $E $M 3
./BfeBenchmark.sh $DATASET $E $M 4 
./BfeBenchmark.sh $DATASET $E $M 5
./BfeBenchmark.sh $DATASET $E $M 6
./BfeBenchmark.sh $DATASET $E $M 7


