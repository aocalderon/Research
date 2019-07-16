#!/bin/bash

N=5
PREFIX_PATH="/home/ubuntu/Research/Datasets/LA/LA_"
FILENAME_SET=( 25 50 100 )
SUFFIX_PATH="K.tsv"
CELLSIZE_SET=( 4 8 16 32 64 )
#CELLSIZE_SET=( 4 )

for n in `seq 1 $N`; do
    for((f=0;f<${#FILENAME_SET[@]};f++)); do
	for((c=0;c<${#CELLSIZE_SET[@]};c++)); do
	    DATASET="${PREFIX_PATH}${FILENAME_SET[f]}${SUFFIX_PATH}"
	    CELLSIZE="${CELLSIZE_SET[c]}"
	    echo "spark-submit --class MF_Grid ~/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.jar --input $DATASET --epsilon 25 --mu 5 --host driver --cores 4 --executors 15 --mfcustomx $CELLSIZE --mfcustomy $CELLSIZE --info --portui 4040 --output ~/tmp/apps03/"
	    spark-submit --class MF_Grid ~/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.jar --input $DATASET --epsilon 25 --mu 5 --host driver --cores 4 --executors 15 --mfcustomx $CELLSIZE --mfcustomy $CELLSIZE --info --portui 4040 --output ~/tmp/apps03/
	done
    done
done
