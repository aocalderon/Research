package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point, Polygon}
import org.locationtech.jts.index.strtree.STRtree
import org.locationtech.jts.io.WKTReader

import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop._

import scala.collection.JavaConverters._
import scala.io.Source
import java.io.FileWriter

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SaveMode}

import edu.ucr.dblab.pflock.sedona.quadtree._
import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.MF_Utils._

import TrajsPartitioner.APoint

object TrajsChecker {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params = new TCParams(args)

    val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .master(params.master())
      .appName("TrajsPartitioner").getOrCreate()
    import spark.implicits._

    implicit val geofactory = new GeometryFactory(new PrecisionModel(1.0/params.tolerance()))
    logger.info(s"START...")

    // Read points from HDFS...
    val pointsDS = spark
      .read
      .option("header", "false")
      .option("delimiter", "\t")
      .csv(params.dataset())
      .map{ row =>
        val oid = row.getString(0).toInt
        val lat = row.getString(1).toDouble
        val lon = row.getString(2).toDouble
        val tid = row.getString(3).toInt
        val cid = row.getString(4).toInt

        APoint(oid, lat, lon, tid, cid)
      }
    logger.info("Read points from HDFS...")

    // Read cells from FS...
    val dataset_name = params.dataset().split("/").last.split("\\.").head
    val base_path    = params.dataset().split("/").reverse.tail.reverse.mkString("/")
    val home         = s"""${System.getenv("HOME")}/Research/local_path"""
    val cells_file   = s"${home}/${base_path}/${dataset_name}/quadtree.wkt"
    
    val reader = new WKTReader(geofactory)
    val buffer = Source.fromFile(cells_file)
    val cellsTree = new STRtree()
    var nCells = 0
    buffer.getLines.toList.foreach{ line =>
      nCells += 1
      val arr      = line.split("\t")
      val envelope = reader.read(arr(0)).asInstanceOf[Polygon].getEnvelopeInternal
      val cellId   = arr(1).toInt

      cellsTree.insert(envelope, cellId)
    }
    buffer.close()
    logger.info("Read cells from FS...")

    // Checking spatial duplicates...
    val tolerance = params.tolerance()
    val duplicates = pointsDS.rdd
      .mapPartitions{ it =>
        it.flatMap{ point =>
          val envelope = point.envelope
          envelope.expandBy(tolerance)
          cellsTree.query(envelope).asScala.toList.map{ cid =>
            (cid.asInstanceOf[Int], point)
          }
        }
      }
      .partitionBy{ SimplePartitioner(nCells) }
      .mapPartitions{ it =>
        val points = it.map(_._2).toList
        val tree = new STRtree()
        points.foreach{ point => tree.insert(point.envelope, point)}
        points.map{ p1 =>
          tree
            .query(p1.expandedEnvelope(tolerance))
            .asScala
            .map(_.asInstanceOf[APoint])
            .toList
            .filter{ p2 => p1.oid != p2.oid && p1.tid == p2.tid}
            .map{ p2 => (p1, p2, p1.distance(p2)) }
        }.flatten.toIterator
      }.cache
    logger.info("Check spatial duplicates...")

    {
      val f = new java.io.FileWriter(s"/tmp/edgesDu_${dataset_name}.wkt")
      f.write(duplicates.map{ case(p1, p2, dist) =>
        s"${p1.oid}\t${p1.tid}\t${p2.oid}\t${p2.tid}\t$dist\n"
      }
        .collect.mkString(""))
      f.close
      logger.info(s"Duplicates|${duplicates.count}")
    }

    spark.close()

    logger.info(s"END.")
  }
}

class TCParams(args: Seq[String]) extends ScallopConf(args) {
  val default_dataset = s"PFlock/LA/LA_50K_sample"

  val tolerance:  ScallopOption[Double]  = opt[Double]  (default = Some(1e-3))
  val dataset:    ScallopOption[String]  = opt[String]  (default = Some(default_dataset))
  val master:     ScallopOption[String]  = opt[String]  (default = Some("local[*]"))

  verify()
}
