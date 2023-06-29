package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point}
import org.locationtech.jts.index.strtree.STRtree

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

import edu.ucr.dblab.pflock.sedona.quadtree._
import edu.ucr.dblab.pflock.Utils._

object CellExplorer {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params = new Params(args)

    implicit val settings = Settings(
      tag = params.tag(),
      epsilon_prime = params.epsilon(),
      capacity = params.capacity(),
      tolerance = params.tolerance()
    )
    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))
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

    val quadtree = timer(s"${settings.appId}|Creating quadtree"){
      Quadtree.getQuadtreeFromPointList(points)
    }
    val points_by_cid = timer(s"${settings.appId}|Feeding Quadtree"){
      points.flatMap{ point =>
        val envelope = point.getEnvelopeInternal
        envelope.expandBy(settings.epsilon)
        val rectangle = new QuadRectangle(envelope)
        quadtree.findZones(rectangle).asScala.map{ cell =>
          val cid = cell.partitionId

          STPoint(point, cid)
        }
      }
    }
    case class Pairs(cid: Int, n: Int)
    val pairs = timer(s"${settings.appId}|Computing distances"){
      points_by_cid.groupBy(_.cid).map{ case(cid, stpoints) =>
        val tree = new STRtree()
        stpoints.foreach{ stpoint =>
          tree.insert(stpoint.point.getEnvelopeInternal, stpoint)
        }
        val pairs = stpoints.map{ stpoint =>
          val envelope = stpoint.point.getEnvelopeInternal
          envelope.expandBy(settings.epsilon)
          tree.query(envelope).asScala.map(_.asInstanceOf[STPoint])
            .filter(_.oid < stpoint.oid).map{ other =>
              stpoint.distance(other)
            }.filter(_ < settings.epsilon)
        }.flatten.toList
        Pairs(cid, pairs.length)
      }
    }

    logger.info("INFO|Saving data|START")
    val ncells = quadtree.getLeafZones.size()
    Quadtree.save(quadtree,
      s"/tmp/q_${settings.tag}_Q${ncells}_E${settings.epsilon.toInt}.wkt")

    val f = new java.io.FileWriter(
      s"/tmp/n_${settings.tag}_Q${ncells}_E${settings.epsilon.toInt}.wkt")

    case class Cell(cid: Int, mbr: String)
    val cells = quadtree.getLeafZones.asScala.map{ leaf =>
      Cell(leaf.partitionId, leaf.wkt())
    }.toList

    case class Count(cid: Int, n: Int)
    val counts = points_by_cid.groupBy(_.cid).map{ case(cid, list) =>
      Count(cid, list.size)
    }

    val N = for{
      cell  <- cells
      count <- counts
      pair  <- pairs

      if(cell.cid == count.cid && cell.cid == pair.cid)

    } yield {
      val wkt = cell.mbr
      val cid = cell.cid
      val npoints = count.n
      val npairs  = pair.n

      s"$wkt\t$cid\t$npoints\t$npairs\n"
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
