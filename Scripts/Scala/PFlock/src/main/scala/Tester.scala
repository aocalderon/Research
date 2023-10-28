package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point, LineString}
import org.locationtech.jts.index.quadtree.{Quadtree => JTSQuadtree}

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

    val spark = SparkSession.builder()
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
    val points = spark.read
      .textFile("/home/and/Research/Datasets/LA_50K_T320.tsv")
      .rdd
      .map{ line =>
        val arr = line.split("\t")
        val x = arr(1).toDouble
        val y = arr(2).toDouble

        val point = geofactory.createPoint(new Coordinate(x, y))
        point.setUserData(line)
        point
      }

    val (cells, tree, mbr) = Quadtree.build(points, fraction = 1, capacity = 250)

    points.map{ point =>
      val id = tree.query(point.getEnvelopeInternal)
        .asScala.map(_.asInstanceOf[Int]).head
      val line = point.getUserData.asInstanceOf[String]

      (id, line)
    }
      .groupBy(_._1)
      .map{ case(cid, list) => (cid, list.map(_._2 + "\n"))}
      .foreach{ case(cid, data) =>
        save(s"/tmp/cell${cid}.tsv"){ data.toList }
      }

    save("/tmp/cells.tsv"){
      cells.map{ case(cid, envelope) =>
        val wkt = geofactory.toGeometry(envelope)

        s"$wkt\t$cid\n"
      }.toList
    }

    /*******************************************************************************/

    spark.close()

    log(s"END|")
  }
}
