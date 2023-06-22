package edu.ucr.dblab.djoin

import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.rogach.scallop._

object Loader {
  def main(args: Array[String]): Unit = {
    implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
    logger.info("Starting session...")
    val params = new LoaderConf(args)
    val spark = SparkSession.builder()
      .appName("Loader")
      .getOrCreate()

    import spark.implicits._
    logger.info("Starting session... Done!")

    val input = params.input()
    logger.info(s"Reading ${input}...")
    val partitions = params.partitions()
    val geometriesTXT = spark.read
      .option("header", params.header())
      .option("delimiter", params.delimiter())
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

class LoaderConf(args: Seq[String]) extends ScallopConf(args) {
  val input: ScallopOption[String] = opt[String]  (required = true)
  val output: ScallopOption[String] = opt[String]  (required = true)
  val partitions: ScallopOption[Int] = opt[Int] (default = Some(1080))
  val header: ScallopOption[Boolean] = opt[Boolean](default = Some(false))
  val delimiter: ScallopOption[String] = opt[String](default = Some("\t"))

  verify()
}
