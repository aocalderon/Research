#!/bin/bash

DATASET_PATH="/home/acald013/Research/Datasets/Berlin/"
DATASET_NAME="berlin0-0"
DATASET_EXT=".tsv"

E=70
spark-submit --class FF /home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon $E --grain_x 100000 --grain_y 100000
spark-submit --class MaximalFinderExpansion /home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --dataset $DATASET_NAME --epsilon $E
E=80
spark-submit --class FF /home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon $E --grain_x 100000 --grain_y 100000
spark-submit --class MaximalFinderExpansion /home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --dataset $DATASET_NAME --epsilon $E
E=90
spark-submit --class FF /home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon $E --grain_x 100000 --grain_y 100000
spark-submit --class MaximalFinderExpansion /home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --dataset $DATASET_NAME --epsilon $E
E=100
spark-submit --class FF /home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon $E --grain_x 100000 --grain_y 100000
spark-submit --class MaximalFinderExpansion /home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --dataset $DATASET_NAME --epsilon $E
E=110
spark-submit --class FF /home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon $E --grain_x 100000 --grain_y 100000
spark-submit --class MaximalFinderExpansion /home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --dataset $DATASET_NAME --epsilon $E
E=120
spark-submit --class FF /home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --input ${DATASET_PATH}${DATASET_NAME}${DATASET_EXT} --epsilon $E --grain_x 100000 --grain_y 100000
spark-submit --class MaximalFinderExpansion /home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar --dataset $DATASET_NAME --epsilon $E
