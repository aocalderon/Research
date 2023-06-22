#!/bin/bash

JAR="$HOME/Research/Scripts/Scala/DistanceJoin/target/scala-2.11/geotester_2.11-0.1.jar"
LIBSPATH="$HOME/Spark/2.4/jars"
LIBS="${LIBSPATH}/geospark-1.2.0.jar"
CLASS="edu.ucr.dblab.djoin.Demo"

scala -cp $LIBS:$JAR $CLASS 

#spark-submit \
#    --master local[1] \
#    --jars $HOME/Spark/2.4/jars/geospark-1.2.0.jar \
#    --class edu.ucr.dblab.djoin.Demo 
