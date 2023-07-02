package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point}
import org.locationtech.jts.index.strtree.STRtree

import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop._

import scala.collection.JavaConverters._

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.Partitioner

import scala.io.Source
import scala.util.Random

import edu.ucr.dblab.pflock.sedona.quadtree.{StandardQuadTree, QuadRectangle}

object LATrajs {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
  case class Data(oid: Int, tid: Int){
    override def toString: String = s"$oid\t$tid"
  }

  def main(args: Array[String]): Unit = {
    implicit val params = new Params(args)

    implicit val spark = SparkSession.builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .master(params.master())
      .appName("LATrajs").getOrCreate()
    import spark.implicits._

    implicit val geofactory = new GeometryFactory(new PrecisionModel(1e-3))

    val input    = params.input()
    val fraction = params.fraction()
    val capacity = params.capacity()
    val maxLevel = params.maxLevel()

    val pointsRaw = spark.read
      .option("delimiter", "\t")
      .option("header", false)
      .textFile(input).rdd
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

    val sample = pointsRaw.sample(false, fraction, 42).collect().toList
    val minx   = pointsRaw.map(_.getX).min
    val miny   = pointsRaw.map(_.getY).min
    val maxx   = pointsRaw.map(_.getX).max
    val maxy   = pointsRaw.map(_.getY).max
    val envelope = new Envelope(new Coordinate(minx, miny), new Coordinate(maxx, maxy))
    val boundary = new QuadRectangle(envelope)
    val quadtree = new StandardQuadTree[Point](boundary, 0, capacity, maxLevel)

    sample.foreach { point =>
      val mbr = new QuadRectangle(point.getEnvelopeInternal)
      quadtree.insert(mbr, point)
    }
    quadtree.assignPartitionIds
    quadtree.assignPartitionLineage
    quadtree.dropElements

    val index = new STRtree()
    val wkt = quadtree.getLeafZones.asScala.map{ leaf: QuadRectangle =>
      val cid = leaf.partitionId.toInt
      val lin = leaf.lineage
      val env = leaf.getEnvelope
      val wkt = geofactory.toGeometry(env).toText

      index.insert(env, cid)

      s"$wkt\t$cid\t$lin\n"
    }

    val f = new java.io.PrintWriter("/tmp/edgesLA.wkt")
    f.write(wkt.mkString(""))
    f.close

    val pointsRDD = pointsRaw.mapPartitionsWithIndex{ case(cid, it) =>
      it.flatMap{ point =>
        val envelope = point.getEnvelopeInternal
        index.query(envelope).asScala.toList.map{ cellId =>
          (cellId.asInstanceOf[Int], point)
        }
      }
    }.partitionBy{
      new Partitioner(){
        def numPartitions: Int = index.size
        def getPartition(key: Any): Int = key.asInstanceOf[Int]
      }
    }.cache

    pointsRDD.map{ case(cid, point) =>
      val userData = point.getUserData.asInstanceOf[Data]
      val oid = userData.oid
      val tid = userData.tid
      val x   = point.getX
      val y   = point.getY

      s"$oid\t$x\t$y\t$tid\t$cid"
    }.toDF("point")
      .write
      .mode(SaveMode.Overwrite)
      .text(params.output())

    spark.close
  }
}

class Params(args: Seq[String]) extends ScallopConf(args) {
  val default_filename =     s"${System.getenv("HOME")}/Research/Datasets/dense.tsv"

  val input:      ScallopOption[String]  = opt[String]  (default = Some(default_filename))
  val capacity:   ScallopOption[Int]     = opt[Int]     (default = Some(250))
  val maxLevel:   ScallopOption[Int]     = opt[Int]     (default = Some(16))
  val fraction:   ScallopOption[Double]  = opt[Double]  (default = Some(0.05))
  val output:     ScallopOption[String]  = opt[String]  (default = Some("/tmp/"))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val master:     ScallopOption[String]  = opt[String]  (default = Some("local[*]"))

  verify()
}
