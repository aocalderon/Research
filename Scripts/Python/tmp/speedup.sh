#!/usr/bin/bash

JAR_FILE="/home/acald013/Research/PFlock/target/scala-2.11/pflock_2.11-2.0.jar"
CORES_PER_NODE=7

NODES=4
ESTART=$1
EEND=$1
ESTEP=5
M=4
D=5

python3 NodesSetter.py -n $NODES
CORES=$(($NODES * $CORES_PER_NODE))
spark-submit --class FlockFinderMergeLast $JAR_FILE \
--path Datasets/Berlin/ --dataset berlin0-10 \
--epsilon $ESTART --epsilon_max $EEND --epsilon_step $ESTEP \
--mu $M --mu_max $M --mu_step 1 \
--delta $D --delta_max $D --delta_step 1 \
--cores $CORES
$SPARK_HOME/sbin/stop-all.sh

python3 NodesSetter.py -n $NODES
CORES=$(($NODES * $CORES_PER_NODE))
spark-submit --class FlockFinderSpatialJoin $JAR_FILE \
--path Datasets/Berlin/ --dataset berlin0-10 \
--epsilon $ESTART --epsilon_max $EEND --epsilon_step $ESTEP \
--mu $M --mu_max $M --mu_step 1 \
--delta $D --delta_max $D --delta_step 1 \
--cores $CORES
$SPARK_HOME/sbin/stop-all.sh
