#!/bin/bash

WORKSPACE="/home/acald013/Research/Scripts/Scala/DistanceJoin/scripts"

cd $WORKSPACE/Test
nohup ./DiskFinderTest_Runner.sh 5 &
cd $WORKSPACE/Debug
nohup ./DiskFinderTestDebug_Runner.sh 1 &
