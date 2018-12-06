#!/bin/bash

DATASET_PATH="/home/acald013/Research/Datasets/Berlin/"
DATASET_NAME="berlin0-0"
DATASET_EXT=".tsv"

ESTART=130
EEND=200
ESTEP=10
R=5
for e in `seq $ESTART $ESTEP $EEND`; do
    for r in `seq 1 $R`; do
	 spark-submit --class MaximalFinderExpansion /home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --dataset $DATASET_NAME --epsilon $e --tag "${e}|Simba|${r}"
	 spark-submit --class Tester /home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.0.jar --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --ppartitions 35 \
	     --dpartitions 35 --epsilon $e --tag "${e}|GeoSpark|${r}"
    done
done
