#!/bin/bash

# Download JDK...
wget http://www.cs.ucr.edu/~acald013/public/tmp/jdk.tar.gz
tar -zxvf jdk.tar.gz
echo "export JAVA_HOME=/home/ubuntu/jdk1.8.0_202" >> ~/.bashrc
echo "export PATH=\$PATH:\$JAVA_HOME/bin" >> ~/.bashrc

# Download Spark...
mkdir Spark
cd Spark
wget http://www.cs.ucr.edu/~acald013/public/tmp/spark.tar.gz
tar -zxvf spark.tar.gz
echo "export SPARK_HOME=/home/ubuntu/Spark/2.4" >> ~/.bashrc
echo "export PATH=\$PATH:\$SPARK_HOME/bin" >> ~/.bashrc

# Reload environment variables...
source ~/.bashrc

