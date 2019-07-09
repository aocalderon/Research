#!/bin/bash

N=5
T_SET=( 15 16 17 18 19 20 )
CELLSIZE_SET=( 100 200 400 600 800 1000)

for n in `seq 1 $N`; do
    for((t=0;t<${#T_SET[@]};t++)); do
	for((c=0;c<${#CELLSIZE_SET[@]};c++)); do
	    T="${T_SET[t]}"
	    CELLSIZE="${CELLSIZE_SET[c]}"
	    echo "spark-submit --class MF ~/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.jar --input ~/Research/Datasets/LA/LA_sample.tsv --epsilon 25 --distance 25 --mu 5 --delta 3 --host driver --cores 4 --executors 15 --timestamp $T --mfcustomx $CELLSIZE --mfcustomy $CELLSIZE --info --portui 4041 --output ~/tmp/apps2/"
	    spark-submit --class MF ~/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.jar --input ~/Research/Datasets/LA/LA_sample.tsv --epsilon 25 --distance 25 --mu 5 --delta 3 --host driver --cores 4 --executors 15 --timestamp $T --mfcustomx $CELLSIZE --mfcustomy $CELLSIZE --info --portui 4041 --output ~/tmp/apps2/
	done
    done
done
