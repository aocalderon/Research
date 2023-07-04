package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point}
import org.locationtech.jts.index.quadtree.{Quadtree => JTSQuadtree}

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import java.io.FileWriter

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.Partitioner

import edu.ucr.dblab.pflock.sedona.quadtree._
import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.MF_Utils._

object QuadtreePartitioner {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params = new BFEParams(args)

    val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .master(params.master())
      .appName("QuadtreePartitioner").getOrCreate()
    import spark.implicits._

    implicit var settings = Settings(
      input = params.input(),
      method = "PFlocks",
      capacity = params.capacity(),
      fraction = params.fraction(),
      tolerance = params.tolerance(),
      debug = params.debug(),
      output = params.output()
    )

    settings.appId = spark.sparkContext.applicationId
    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))

    printParams(args)
    log(s"START|")

    val ( (pointsRaw, nRead), tRead) = timer{
      val pointsRaw = spark.read
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

    val ( (quadtree, cells), tIndex) = timer{
      val quadtree = Quadtree.getQuadtreeFromPoints(pointsRaw, expand = false)
      val cells = quadtree.getLeafZones.asScala.map{ leaf =>
        val cid = leaf.partitionId
        val lin = leaf.lineage
        val env = leaf.getEnvelope

        cid -> Cell(env, cid, lin)
      }.toMap

      (quadtree, cells)
    }
    val nIndex = cells.size
    log(s"Index|$nIndex")
    logt(s"Index|$tIndex")

    case class Record(cid: Int, point: String)
    val ( (pointsRDD, nShuffle), tShuffle) = timer{
      val pointsRDD = pointsRaw.mapPartitions{ points =>
        points.flatMap{ point =>
          val envelope = point.getEnvelopeInternal
          val rectangle = new QuadRectangle(envelope)
          quadtree.findZones(rectangle).asScala.map{ cell =>
            val cid = cell.partitionId.toInt

            (cid, STPoint(point, cid).toString)
          }
        }
      }.partitionBy(new Partitioner {
        def numPartitions: Int = cells.size
        def getPartition(key: Any): Int = key.asInstanceOf[Int]
      }).cache
      val nShuffle = pointsRDD.count
      (pointsRDD, nShuffle)
    }
    log(s"Shuffle|$nShuffle")
    logt(s"Shuffle|$tShuffle")

    // Save quadtree's cells to disk and data to HDFS or Local...
    val (_, tSave) = timer{
      val (hdfs_name, fs_name) = getHDFSandFSnames("quadtree", "wkt")
      Quadtree.save(quadtree, fs_name) 
      if(params.master() == "yarn"){
        pointsRDD.map(_._2).toDF("point").write
          .mode(SaveMode.Overwrite)
          .text(hdfs_name)
        log("Save|HDFS")
      } else {
        val filename = settings.input.split("/").last.split("\\.")
        filename.foreach{println}
        val fs_path = settings.input.split("/").reverse.tail.reverse.mkString("/")
        val f = new FileWriter(s"${fs_path}/${filename(0)}_partitioned.${filename(1)}")
        f.write( pointsRDD.map(_._2 + "\n").collect.mkString("") )
        f.close
        log("Save|Local")
      }
    }
    logt(s"Save|$tSave")

    spark.close()

    log(s"END|")
  }
}
