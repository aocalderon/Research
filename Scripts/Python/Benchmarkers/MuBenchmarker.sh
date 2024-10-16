#!/usr/bin/bash

JAR_FILE="/home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar"
CORES_PER_NODE=7
SCRIPT_NAME="MuBenchmarker"

NODES=4
DATAPATH="Datasets/Berlin/"
DATASET="berlin0-10"
M=$1
D=6
E=100

#ID=`date +%s`
#TIMESTAMP=`date`
#echo "FLOCKFINDER=SpatialJoin;TIME=$TIMESTAMP;RUN=$ID;NODES=$NODES;ESTART=$ESTART;EEND=$EEND;ESTEP=$ESTEP;MU=$M;DELTA=$D;SCRIPT=$SCRIPT_NAME;EVENT=Start"
#python3 NodesSetter.py -n $NODES
#CORES=$(($NODES * $CORES_PER_NODE))
#spark-submit --class FlockFinderSpatialJoin $JAR_FILE \
#--path $DATAPATH --dataset $DATASET \
#--epsilon $E --epsilon_max $E --epsilon_step $E \
#--mu $MSTART --mu_max $MEND --mu_step $MSTEP \
#--delta $D --delta_max $D --delta_step $D \
#--cores $CORES
#$SPARK_HOME/sbin/stop-all.sh
#TIMESTAMP=`date`
#echo "FLOCKFINDER=SpatialJoin;TIME=$TIMESTAMP;RUN=$ID;NODES=$NODES;ESTART=$ESTART;EEND=$EEND;ESTEP=$ESTEP;MU=$M;DELTA=$D;SCRIPT=$SCRIPT_NAME;EVENT=End"

ID=`date +%s`
TIMESTAMP=`date`
echo "FLOCKFINDER=MergeLast;TIME=$TIMESTAMP;RUN=$ID;NODES=$NODES;EPSILON=$E;MU=$M;DELTA=$D;SCRIPT=$SCRIPT_NAME;EVENT=Start"
python3 NodesSetter.py -n $NODES
CORES=$(($NODES * $CORES_PER_NODE))
START=`date +%s`
spark-submit --class FlockFinderMergeLast $JAR_FILE \
--path $DATAPATH --dataset $DATASET \
--epsilon $E --epsilon_max $E \
--mu $M --mu_max $M \
--delta $D --delta_max $D \
--cores $CORES
END=`date +%s`
$SPARK_HOME/sbin/stop-all.sh
TIMESTAMP=`date`
TIMER=$(($END - $START))
echo "FLOCKFINDER=MergeLast;TIME=$TIMESTAMP;RUN=$ID;NODES=$NODES;EPSILON=$E;MU=$M;DELTA=$D;SCRIPT=$SCRIPT_NAME;EVENT=End;TIMER=$TIMER"
