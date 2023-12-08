#/usr/bin/bash 

PARAMS=(
 --files  /home/acald013/Spark/2.4/conf/log4j.properties   \
 --conf   spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/home/acald013/Spark/2.4/conf/log4j.properties   \
 --master local[*]  \
 --class  edu.ucr.dblab.streaming.StreamTester  
)

spark-submit ${PARAMS[@]} target/scala-2.11/streaming_2.11-0.1.0.jar $* 
