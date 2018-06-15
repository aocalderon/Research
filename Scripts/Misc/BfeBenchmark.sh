#!/bin/bash

DATASET=$1
EPSILON=$2
MU=$3
DELTA=$4
SCRIPT_NAME="BfeBenchmark"
FILENAME=$(basename -- "$DATASET")
EXTENSION="${FILENAME##*.}"
FILENAME="${FILENAME%.*}"

ID=`date +%s`
TIMESTAMP=`date`
echo "FLOCKFINDER=bfe;TIME=$TIMESTAMP;RUN=$ID;EPSILON=$EPSILON;MU=$MU;DELTA=$DELTA;SCRIPT=$SCRIPT_NAME;EVENT=Start"
START=`date +%s`
time ~/opt/BFE_Modified/build/flocks -m BFE \
	-d $EPSILON \
	-s $MU \
	-l $DELTA \
	-f $DATASET > /tmp/bfe.log
END=`date +%s`
FILENAME="BFE_N${FILENAME}_E${EPSILON}_M${MU}_D${DELTA}.txt"
cat /tmp/bfe.log | grep -P "\d\, \d+\," > /tmp/$FILENAME
grep "Closing stream" /tmp/bfe.log
grep "totalTime" /tmp/bfe.log
ID=`date +%s`
TIMESTAMP=`date`
TIMER=$(($END - $START))
echo "FLOCKFINDER=bfe;TIME=$TIMESTAMP;RUN=$ID;EPSILON=$EPSILON;MU=$MU;DELTA=$DELTA;SCRIPT=$SCRIPT_NAME;EVENT=End;TIMER=$TIMER"

