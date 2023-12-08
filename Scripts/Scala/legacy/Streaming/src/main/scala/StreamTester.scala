package edu.ucr.dblab.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark._
import org.apache.spark.streaming._
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Timestamp

object StreamTester {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]){
    val spark = SparkSession.builder()
      .appName("Streaming")
      .getOrCreate()
    import spark.implicits._

    val schema = new StructType()
      .add("t", IntegerType, false)
      .add("x", DoubleType, false)
      .add("y", DoubleType, false)
      .add("pids", StringType, false)
      .add("ts", TimestampType, false)
    val SSDF = spark.readStream.schema(schema)
      .option("delimiter","\t")
      .csv("file:///home/acald013/Datasets/ICPE/Demo/out")
    logger.info(s"Is pointsSSDF streaming... ${SSDF.isStreaming}")

    val stream = SSDF.as[(Int, Double, Double, String, Timestamp)]

    val count = stream.withWatermark("ts", "0 seconds")
      .groupBy(
        window($"ts", "1 seconds", "1 seconds"), 
        $"t"
      )
      .count()
      .orderBy($"window", $"t")

    val query = count.writeStream.format("console")
      .option("truncate", "false")
      .outputMode(OutputMode.Complete())
      .start()

    query.awaitTermination()

  }
}
