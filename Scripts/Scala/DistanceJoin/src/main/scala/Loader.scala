package edu.ucr.dblab.djoin

import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator;
import com.vividsolutions.jts.io.WKTReader
import com.vividsolutions.jts.geom.{Geometry, Polygon, Coordinate, GeometryFactory, PrecisionModel}
import org.rogach.scallop._

object Loader {
  def main(args: Array[String]): Unit = {
    implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
    logger.info("Starting session...")
    val params = new TranslateConf(args)
    val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .appName("GeoTemplate")
      .getOrCreate()

    import spark.implicits._
    logger.info("Starting session... Done!")

    val input = params.input()
    logger.info(s"Reading ${input}...")
    val partitions = params.partitions()
    val geometriesTXT = spark.read
      .option("header", true)
      .option("delimiter", "\t")
      .csv(input)
      .repartition(partitions).cache()
    logger.info(s"Reading ${input}... Done!")
    logger.info(s"Number of records: ${geometriesTXT.count()}")

    val output = params.output()
    logger.info(s"Saving to ${output}...")
    geometriesTXT.write
      .mode(SaveMode.Overwrite)
      .option("header", false)
      .option("delimiter", "\t")
      .format("csv")
      .save(output)
    logger.info(s"Saving to ${output}... Done!")

    logger.info("Closing session...")
    spark.close()
    logger.info("Closing session... Done!")
  }
}

class TranslateConf(args: Seq[String]) extends ScallopConf(args) {
  val input: ScallopOption[String] = opt[String]  (required = true)
  val output: ScallopOption[String] = opt[String]  (required = true)
  val partitions: ScallopOption[Int] = opt[Int] (default = Some(1080))

  verify()
}
