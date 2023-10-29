package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point, LineString}
import org.locationtech.jts.index.quadtree.{Quadtree => JTSQuadtree}
import org.locationtech.jts.index.strtree.STRtree

import org.slf4j.{Logger, LoggerFactory}

import scala.xml._
import scala.collection.JavaConverters._
import java.io.FileWriter

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SaveMode}

import edu.ucr.dblab.pflock.MF_Utils._
import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.sedona.quadtree.Quadtree

object Tester {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params = new BFEParams(args)

    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .master(params.master())
      .appName("Tester").getOrCreate()
    import spark.implicits._

    implicit var settings = Settings(
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

    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))

    printParams(args)
    log(s"START|")

    /*******************************************************************************/
    // Code here...
    def readPoints(filename: String)
      (implicit spark: SparkSession, geofactory: GeometryFactory): RDD[Point] = {

      spark.read
        .textFile(filename)
        .rdd
        .map{ line =>
          val arr = line.split("\t")
          val x = arr(1).toDouble
          val y = arr(2).toDouble

          val point = geofactory.createPoint(new Coordinate(x, y))
          point.setUserData(line)
          point
        }
    }

    def savePoints(points: RDD[Point], tree: STRtree, tag: String = "T",
      outpath: String = "/tmp/"): Map[Int, Int] = {

      points.map{ point =>
        val id = tree.query(point.getEnvelopeInternal).asScala.map(_.asInstanceOf[Int]).head
        val line = point.getUserData.asInstanceOf[String]

        (id, line)
      }
        .groupBy(_._1)
        .map{ case(cid, list) =>
          val data = list.map(_._2 + "\n").toList
          save(s"${outpath}${tag}_cell${cid}.tsv"){ data }

          (cid -> data.size)
        }.collect.toMap
    }

    val outpath = "/home/and/Research/Datasets/LA"

    val points1 = readPoints("/home/and/Research/Datasets/LA_50K_T320.tsv")
    val points2 = readPoints("/home/and/Research/Datasets/LA_50K_T321.tsv")

    val (cells, tree, mbr) = Quadtree.build(points1 ++ points2, fraction = 1, capacity = 250)

    val counts1 = savePoints(points1, tree, "T320", outpath + "/cells/")
    val counts2 = savePoints(points2, tree, "T321", outpath + "/cells/")

    val data = cells.map{ case(cid, envelope) =>
      val polygon = geofactory.toGeometry(envelope)
      val area = polygon.getArea
      val wkt  = polygon.toText
      val n1   = counts1.getOrElse(cid, 0)
      val n2   = counts2.getOrElse(cid, 0)
      val density1 = if(n1 > 0) n1.toDouble / area else 0
      val density2 = if(n2 > 0) n2.toDouble / area else 0

      s"$wkt\t$cid\t$area\t$n1\t$density1\t$n2\t$density2\n"
    }.toList

    save(s"${outpath}/cells.tsv"){ data }

    /*******************************************************************************/

    spark.close()

    log(s"END|")
  }
}
