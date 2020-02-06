package edu.ucr.dblab

import org.slf4j.{LoggerFactory, Logger}
import org.rogach.scallop._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.utils.GeoSparkConf
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import edu.ucr.dblab.Utils._

object GeoTesterDF{
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class PointWKT(wkt: String, id: Int, t: Int)

  case class ST_Point(id: Int, x: Double, y: Double, t: Int){
    def asWKT = PointWKT(s"POINT($x $y)", id, t)
  }

  def n(count: Long, name: String = ""): Unit = logger.info(s"$name: $count")

  def main(args: Array[String]): Unit = {
    logger.info("Starting session...")
    implicit val params = new GeoTesterConf(args)
    implicit val spark = SparkSession.builder()
      .appName("GeoTesterDF")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .config("geospark.join.numpartition", params.partitions())
      .config("geospark.global.indextype", params.indextype())
      .config("geospark.join.gridtype", params.gridtype())
      .getOrCreate()
    import spark.implicits._
    GeoSparkSQLRegistrator.registerAll(spark)
    implicit val conf = spark.sparkContext.getConf
    logger.info(s"${appId}|${System.getProperty("sun.java.command")}")
    def getConf(property: String)(implicit conf: SparkConf): String = conf.get(property)
    def appId: String = if(getConf("spark.master").contains("local")){
      getConf("spark.app.id")
    } else {
      getConf("spark.app.id").takeRight(4)
    }
    def header(msg: String): String = s"GeoTester|${appId}|$msg"

    logger.info("Starting session... Done!")

    val pointsRaw = timer{"Reading data"}{
      val pointsSchema = ScalaReflection.schemaFor[ST_Point].dataType.asInstanceOf[StructType]
      val points = spark.read.schema(pointsSchema)
        .option("delimiter", "\t").option("header", false)
        .csv(params.input()).as[ST_Point].cache
      n(points.count(), "Raw points")
      points
    }

    val points = timer{header("Casting geometries")}{
      pointsRaw.createOrReplaceTempView("points")
      val sql = """
        |SELECT 
        |  ST_Point(CAST(x AS Decimal(20,3)), CAST(y AS Decimal(20,3))) AS geom, id, t 
        |FROM 
        |  points
        """.stripMargin
      val points = spark.sql(sql)
      points.cache
      points
    }

    val distance = params.epsilon() + params.precision()
    val pairs = timer{header("Distance self-join")}{
      points.createOrReplaceTempView("points")
      val sql = s"""
        |SELECT
        |  *
        |FROM
        |  points A, points B
        |WHERE
        |  ST_DISTANCE(A.geom, B.geom) <= ${distance}
        """.stripMargin
      val pairs = spark.sql(sql).filter(f => f.getInt(1) < f.getInt(4)).cache
      n(pairs.count(), "Pairs")
      pairs
    }
    
    logger.info("Closing session...")
    spark.close()
    logger.info("Closing session... Done!")
  }
}

class GeoTesterConf(args: Seq[String]) extends ScallopConf(args) {
  val input = opt[String](default = Some(""))
  val epsilon = opt[Double](default = Some(10.0))
  val precision = opt[Double](default = Some(0.001))
  val partitions = opt[Int](default = Some(256))
  val indextype = opt[String](default = Some("rtree"))
  val gridtype = opt[String](default = Some("kdbtree"))

  verify()
}
