#!/bin/bash

JAR="$HOME/Research/Scripts/Scala/GraphViewer/target/scala-2.11/graphviewer_2.11-0.1.jar"
LIBSPATH="$HOME/Spark/2.4/jars"
LIBS="${LIBSPATH}/JTSplus-0.1.4.jar"
CLASS="edu.ucr.dblab.FlockCompare"

scala -cp $LIBS:$JAR $CLASS $1 $2 
