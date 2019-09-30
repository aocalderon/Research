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
      .config("spark.default.parallelism", 3 * 12 * 10)
      //.config("spark.serializer",classOf[KryoSerializer].getName)
      //.config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .config("spark.scheduler.mode", "FAIR")
      //.config("spark.cores.max", cores * executors)
      //.config("spark.executor.cores", cores)
      //.master(master)
      .appName("PatternEnumerator")
      .getOrCreate()
    import spark.implicits._

    val pointSchema = new StructType()
      .add("id", IntegerType, false)
      .add("x", DoubleType, false)
      .add("y", DoubleType, false)
      .add("t", IntegerType, false)
    val pointsSSDF = spark.readStream.schema(pointSchema)
      .option("delimiter","\t")
      .csv("/tmp/points")
    logger.info(s"Is pointsSSDF streaming... ${pointsSSDF.isStreaming}")

    val currentPoints = pointsSSDF.withColumn("time", current_timestamp())
    val stream = currentPoints.as[(Int, Double, Double, Int, Timestamp)]

    val countPoints = stream.groupBy(window($"time", "10 minutes"), $"id")
      .count()
      .orderBy($"window")

    val query = countPoints.writeStream.format("console")
      .option("truncate", "false")
      .outputMode(OutputMode.Complete())
      .start()

    query.awaitTermination()

  }
}
