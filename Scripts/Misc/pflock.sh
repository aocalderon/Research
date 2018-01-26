#!/bin/bash

PPATH=$1
DATASET=$2
EPSILON=$3
MU=$4
DELTA=$5

spark-submit /home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar \
	--partitions 16 \
	--epsilon $EPSILON \
	--mu $MU \
	--delta $DELTA \
	--path $PPATH \
	--dataset $DATASET

FILENAME="PFLOCK_E${EPSILON}_M${MU}_D${DELTA}.txt"

cat /tmp/$FILENAME

scp -i ~/.ssh/id_rsa /tmp/$FILENAME acald013@bolt.cs.ucr.edu:/home/csgrads/acald013/public_html/public/data/
