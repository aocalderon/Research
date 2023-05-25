package edu.ucr.dblab.pflock

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Envelope, Coordinate, Point}
import org.datasyslab.geospark.spatialRDD.SpatialRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

import edu.ucr.dblab.pflock.quadtree._

object CellExplorer {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params = new Params(args)
    implicit val geofactory = new GeometryFactory(new PrecisionModel(params.tolerance()))

    val tag = params.tag()
    val properties = System.getProperties().asScala
    logger.info(s"COMMAND|${properties("sun.java.command")}")
    
    logger.info("INFO|Reading data|START")
    import scala.io.Source
    val pointsBuffer = Source.fromFile(params.input())
    val points = pointsBuffer.getLines.map { line =>
      val arr = line.split("\t")
      val pid = arr(0)
      val x = arr(1).toDouble
      val y = arr(2).toDouble
      val tid = arr(3)
      val point = geofactory.createPoint(new Coordinate(x, y))
      point.setUserData(s"$pid\t$tid")
      point
    }.toList
    pointsBuffer.close()
    logger.info("INFO|Reading data|END")

    logger.info("INFO|Creating quadtree|START")
    val itemsPerNode = params.capacity()
    val quadtree = Quadtree.getQuadtreeFromPoints(points, maxItemsPerNode = itemsPerNode)
    logger.info("INFO|Creating quadtree|END")

    logger.info("INFO|Saving data|START")
    val ncells = quadtree.getLeafZones.size()
    Quadtree.save(quadtree, s"/tmp/q_${tag}_Q${ncells}.wkt")

    val f = new java.io.FileWriter(s"/tmp/n_${tag}_Q${ncells}.wkt")

    case class Cell(cid: Int, mbr: String)
    val cells = quadtree.getLeafZones.asScala.map{ leaf =>
      Cell(leaf.partitionId, leaf.wkt())
    }.toList

    case class Count(cid: Int, n: Int)
    val counts = points.flatMap{ point =>
      quadtree.findZones(new QuadRectangle(point.getEnvelopeInternal)).asScala.map{ cell =>
        val cid = cell.partitionId

        (1, cid)
      }
    }.groupBy(_._2).map{ case(cid, list) =>
      Count(cid, list.size)
    }

    val N = for{
      cell  <- cells
      count <- counts if(cell.cid == count.cid)
        } yield {
      val wkt = cell.mbr
      val cid = cell.cid
      val   n = count.n

      s"$wkt\t$cid\t$n\n"
    }
   
    f.write(N.mkString(""))
    f.close()

    logger.info("INFO|Saving data|END")
  }
}

import org.rogach.scallop._

class Params(args: Seq[String]) extends ScallopConf(args) {
  val tolerance: ScallopOption[Double]  = opt[Double]  (default = Some(1e-3))
  val input:     ScallopOption[String]  = opt[String]  (default = Some(""))
  val epsilon:   ScallopOption[Double]  = opt[Double]  (default = Some(10.0))
  val mu:        ScallopOption[Int]     = opt[Int]     (default = Some(5))
  val capacity:  ScallopOption[Int]     = opt[Int]     (default = Some(100))
  val tag:       ScallopOption[String]  = opt[String]  (default = Some(""))
  val output:    ScallopOption[String]  = opt[String]  (default = Some("/tmp"))
  val debug:     ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}



/*
 val spark = SparkSession.builder()
 .config("spark.serializer",classOf[KryoSerializer].getName)
 .appName("CellExplorer")
 .getOrCreate()
 import spark.implicits._
 val points = spark.read
 .option("delimiter", "\t")
 .option("header", false)
 .textFile("file:///home/acald013/Datasets/LA/LA_50K_T316.tsv")

 points.show()

 spark.close()
 */
