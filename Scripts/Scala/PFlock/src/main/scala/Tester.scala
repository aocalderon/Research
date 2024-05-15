package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.MF_Utils._
import edu.ucr.dblab.pflock.Utils._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.{GeometryFactory, PrecisionModel}
import org.locationtech.jts.index.quadtree.{Quadtree => JTSQuadtree}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.xml._

object Tester {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params = new BFEParams(args)

    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .master(params.master())
      .appName("Tester").getOrCreate()
    import spark.implicits._

    implicit val S = Settings(
      dataset = params.dataset(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = "PFlocks",
      capacity = params.capacity(),
      fraction = params.fraction(),
      tolerance = params.tolerance(),
      tag = params.tag(),
      debug = params.debug(),
      output = params.output(),
      appId = spark.sparkContext.applicationId
    )

    implicit val geofactory = new GeometryFactory(new PrecisionModel(S.scale))

    printParams(args)
    log(s"START|")

    /*******************************************************************************/
    // Code here...
    val trajs = spark.read
      .option("header", false)
      .option("delimiter", "\t")
      .csv(S.dataset)
      .mapPartitions{ rows =>
        rows.map{ row =>
          val oid = row.getString(0).toInt
          val lon = row.getString(1).toDouble
          val lat = row.getString(2).toDouble
          val tid = row.getString(3).toInt

          (oid, lon, lat, tid)
        }
      }.toDF("oid", "lon", "lat", "tid")
      .repartition($"tid").map{ row =>
        val o = row.getInt(0)
        val x = row.getDouble(1)
        val y = row.getDouble(2)
        val t = row.getInt(3)

        s"$o\t$x\t$y\t$t"}
      .write.text("/tmp/berlin")

    /*******************************************************************************/

    spark.close()

    log(s"END|")
  }
}
