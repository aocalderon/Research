package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point}
import org.locationtech.jts.index.quadtree.{Quadtree => JTSQuadtree}

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SaveMode}

import edu.ucr.dblab.pflock.Utils._

import dbis.stark._
import dbis.stark.spatial.partitioner.BSPartitioner
import org.apache.spark.SpatialRDD._

object Tester {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params = new BFEParams(args)

    val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .appName("Tester").getOrCreate()
    import spark.implicits._

    implicit var settings = Settings(
      input = params.input(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = "PFlocks",
      capacity = params.capacity(),
      fraction = params.fraction(),
      tolerance = params.tolerance(),
      tag = params.tag(),
      debug = params.debug(),
      output = params.output()
    )

    settings.appId = spark.sparkContext.applicationId
    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))

    printParams(args)
    log(s"START|")

    val ( (pointsRaw, nRead), tRead) = timer{
      val pointsRaw = spark.read
        .option("delimiter", "\t")
        .option("header", false)
        .textFile(settings.input).rdd
        .map { line =>
          val arr = line.split("\t")
          val i = arr(0).toInt
          val x = arr(1).toDouble
          val y = arr(2).toDouble
          val t = arr(3).toInt
          val point = geofactory.createPoint(new Coordinate(x, y))
          point.setUserData(Data(i, t))
          point
        }.cache
      val nRead = pointsRaw.count
      (pointsRaw, nRead)
    }
    log(s"Read|$nRead")
    logt(s"Read|$tRead")

    val test = pointsRaw.map{ point =>
      (STObject( point.toText ), point)
    }
    val bsp = BSPartitioner(test, params.epsilon(), params.capacity(), pointsOnly = true)
    bsp.printPartitions("/tmp/edgesPStark.wkt")
    val pointsRDD = test.partitionBy(bsp).cache
    pointsRDD.count

    save("/tmp/edgesPStark.wkt"){
      (0 until bsp.numPartitions).map{ i =>
        val cell = bsp.partitionBounds(i)
        val wkt = cell.extent.wkt
        val cid = cell.id

        s"$wkt\t$cid\n"
      }
    }

    spark.close()

    log(s"END|")
  }
}
