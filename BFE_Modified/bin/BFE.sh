#!/bin/bash

DATASET=$1
EPSILON=$2
MU=$3
DELTA=$4

/opt/BFE_Modified/cmake-build-debug/flocks -m BFE \
	-d $EPSILON \
	-s $MU \
	-l $DELTA \
	-f $DATASET > /tmp/bfe.log

FILENAME="BFE_E${EPSILON}_M${MU}_D${DELTA}.txt"
cat /tmp/bfe.log | grep -P "\d\, \d+\," > /tmp/$FILENAME

cat /tmp/$FILENAME

scp -i ~/.ssh/dblab /tmp/$FILENAME acald013@bolt.cs.ucr.edu:/home/csgrads/acald013/public_html/public/data/

grep "Closing stream" /tmp/bfe.log

