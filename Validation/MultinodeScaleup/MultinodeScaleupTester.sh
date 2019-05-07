#!/bin/bash

N=5
FFTIMESTAMP=6
for i in `seq 1 $N`; do
	 echo "spark-submit --class FF /home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.jar --input ~/Research/Datasets/Berlin/SideBySide/B3.tsv --epsilon 110 --distance 100 --mu 3 --delta 3 --master 169.235.27.138 --executors 3 --cores 4 --spatial CUSTOM --customymf 30 --customxmf 90 --customy 30 --customx 90 --fftimestamp $FFTIMESTAMP"
	 spark-submit --class FF /home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.jar --input ~/Research/Datasets/Berlin/SideBySide/B3.tsv --epsilon 110 --distance 100 --mu 3 --delta 3 --master 169.235.27.138 --executors 3 --cores 4 --spatial CUSTOM --customymf 30 --customxmf 90 --customy 30 --customx 90 --fftimestamp $FFTIMESTAMP

	 echo "spark-submit --class FF /home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.jar --input ~/Research/Datasets/Berlin/SideBySide/B1.tsv --epsilon 110 --distance 100 --mu 3 --delta 3 --master 169.235.27.138 --executors 1 --cores 4 --spatial CUSTOM --customymf 30 --customxmf 30 --customy 30 --customx 30 --fftimestamp $FFTIMESTAMP"
	 spark-submit --class FF /home/acald013/Research/GeoSpark/target/scala-2.11/pflock_2.11-0.1.jar --input ~/Research/Datasets/Berlin/SideBySide/B1.tsv --epsilon 110 --distance 100 --mu 3 --delta 3 --master 169.235.27.138 --executors 1 --cores 4 --spatial CUSTOM --customymf 30 --customxmf 30 --customy 30 --customx 30 --fftimestamp $FFTIMESTAMP
done