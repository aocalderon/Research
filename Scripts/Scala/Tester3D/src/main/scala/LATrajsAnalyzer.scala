package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point}
import org.locationtech.jts.io.WKTReader
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

object LATrajsAnalyzer {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
  case class Data(oid: Int, tid: Int){
    override def toString: String = s"$oid\t$tid"
  }
  case class STPoint(oid: Int, lat: Double, lon: Double, t: Int, cellId: Int = -1)
      extends Coordinate(lat, lon) {
    
    def jts(implicit G: GeometryFactory): Point = {
      val point = G.createPoint(new Coordinate(x, y))
      point.setUserData(Data(oid, t))
      point
    }

    override def toString: String = s"$oid\t$lat\t$lon\t$t\t$cellId"
  }

  def main(args: Array[String]): Unit = {
    implicit val params = new AParams(args)

    implicit val spark = SparkSession.builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .master(params.master())
      .appName("LATrajsAnalyzer").getOrCreate()
    import spark.implicits._

    implicit val geofactory = new GeometryFactory(new PrecisionModel(1e-3))

    logger.info("Reading cells...")
    val buffer = Source.fromFile(params.cells())
    val reader = new WKTReader(geofactory)
    val cells = buffer.getLines.map{ line =>
      val arr = line.split("\t")
      val wkt = arr(0)
      val env = reader.read(wkt).getEnvelopeInternal
      val cid = arr(1).toInt

      (cid, env)
    }.toList
    buffer.close
    val numPartitions = cells.maxBy(_._1)._1
    logger.info(s"Number of partitions: $numPartitions")

    val (left, down) = (cells.map{_._2.getMinX}.min, cells.map{_._2.getMinY}.min)
    logger.info(s"Cells: ${cells.size}")
    logger.info(s"($left, $down)")
    val dataset = params.dataset()

    logger.info("Reading data...")
    val pointsRDD = spark.read
      .option("header", false)
      .textFile(dataset)
      .rdd.map { line =>
        val arr = line.split("\t")
        val i = arr(0).toInt
        val x = arr(1).toDouble - left
        val y = arr(2).toDouble - down
        val t = arr(3).toInt
        val c = arr(4).toInt

        (c, STPoint(i, x, y, t, c) )
      }.partitionBy{
        new Partitioner(){
          def numPartitions: Int = cells.size
          def getPartition(key: Any): Int = key.asInstanceOf[Int]
        }
      }.map(_._2).cache
    logger.info(s"Read|${pointsRDD.count}")

    if(params.saves()){
      if(params.master() == "yarn"){
        pointsRDD.map(_.toString).toDF.write
          .mode(SaveMode.Overwrite)
          .text(s"${params.dataset()}_shifted")
        logger.info(s"SaveHDFS|${pointsRDD.count}")
      } else {
        val f = new java.io.PrintWriter(params.output())
        f.write( pointsRDD.map(_.toString + "\n").collect.mkString("") )
        f.close
        logger.info(s"SaveLocal|${pointsRDD.count}")
      }
    }

    if(params.debug()){
      import org.locationtech.jts.algorithm.ConvexHull
      val hulls = pointsRDD.mapPartitionsWithIndex{ case(i, it) =>
        val wkt = new ConvexHull(it.toArray, geofactory).getConvexHull.toText
        Iterator( s"$wkt\t$i\n" )
      }.collect
      val f = new java.io.PrintWriter("/tmp/edgesCH.wkt")
      f.write(hulls.mkString(""))
      f.close
      logger.info(s"Save|${hulls.size}")
    }

    val tolerance = params.tolerance()
    val duplicates = pointsRDD.mapPartitions{ it =>
      val points = it.toList
      val tree = new STRtree()
      points.foreach{ point => tree.insert(new Envelope(point), point)}
      points.map{ point =>
        val envelope = new Envelope(point)
        envelope.expandBy(tolerance)
        val hood = tree.query(envelope).asScala.map(_.asInstanceOf[STPoint]).toList
        hood.filter(_.oid != point.oid).map{ p2 => (point, p2, point.distance(p2)) }
      }.flatten.toIterator
    }.cache

    {
      val f = new java.io.PrintWriter("/tmp/edgesDu.wkt")
      f.write(duplicates.map{ case(p1, p2, dist) => s"${p1.oid}\t${p2.oid}\t$dist\n" }
        .collect.mkString(""))
      f.close
      logger.info(s"Duplicates|${duplicates.count}")
    }

    spark.close
  }
}

class AParams(args: Seq[String]) extends ScallopConf(args) {
  val default_dataset = s"PFlock/LA/LA_50K_T320"
  val default_cells   = s"/home/acald013/Research/local_path/PFlock/LA/quadtreeLA_50K_T320.wkt"

  val dataset:   ScallopOption[String]  = opt[String]  (default = Some(default_dataset))
  val cells:     ScallopOption[String]  = opt[String]  (default = Some(default_cells))
  val tolerance: ScallopOption[Double]  = opt[Double]  (default = Some(1e-3))
  val output:    ScallopOption[String]  = opt[String]  (default = Some("/tmp/"))
  val saves:     ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:     ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val master:    ScallopOption[String]  = opt[String]  (default = Some("local[*]"))

  verify()
}
