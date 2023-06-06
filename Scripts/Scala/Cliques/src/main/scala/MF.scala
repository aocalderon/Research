package edu.ucr.dblab.pflock

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Envelope, Coordinate, Point}

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.Partitioner

import edu.ucr.dblab.pflock.quadtree._
import edu.ucr.dblab.pflock.Utils._

object MF {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params = new BFEParams(args)
    implicit val d: Boolean = params.debug()

    val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .appName("MF")
      .getOrCreate()
    import spark.implicits._

    implicit var settings = Settings(
      input = params.input(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = params.method(),
      capacity = params.capacity(),
      appId = spark.sparkContext.applicationId,
      tolerance = params.tolerance(),
      tag = params.tag()
    )

    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))

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
      }
    log(s"Reading data|START")

    val (quadtree, cells) = timer(s"Creating quadtree"){
      val sample = pointsRaw.sample(false, 0.1, 42).collect().toList
      val quadtree = Quadtree.getQuadtreeFromPoints(sample, capacity = settings.capacity)
      val cells = quadtree.getLeafZones.asScala.map{ leaf =>
        val cid = leaf.partitionId
        val lin = leaf.lineage
        val env = leaf.getEnvelope

        cid -> Cell(env, cid, lin)
      }.toMap
      (quadtree, cells)
    }

    val pointsRDD = timer(s"Partitioning data"){
      pointsRaw.mapPartitions{ points =>
        points.flatMap{ point =>
          val envelope = point.getEnvelopeInternal
          envelope.expandBy(settings.epsilon)
          val rectangle = new QuadRectangle(envelope)
          quadtree.findZones(rectangle).asScala.map{ cell =>
            val cid = cell.partitionId

            (cid, STPoint(point, cid))
          }
        }
      }.partitionBy(new Partitioner {
        def numPartitions: Int = quadtree.getTotalNumLeafNode
        def getPartition(key: Any): Int = key.asInstanceOf[Int]
      }).map(_._2).mapPartitionsWithIndex{ case(cid, it) =>
        setCount(it.toList).toIterator
      }
    }
    pointsRDD.count

    val maximalsRDD = timer{"Runing BFE"}{
      val M = pointsRDD.mapPartitionsWithIndex{ case(cid, it) =>
        val cell = cells(cid)
        val points = it.toList
        val (maximals, stats) = BFE.runParallel(points, cell)

        maximals.entries.map(_.value)
      }
      M.count()
      M
    }

    debug{
      save("/tmp/edgesPoints.wkt"){
        pointsRDD.mapPartitionsWithIndex{ case(cid, it) =>
          it.map{ p => s"${p.wkt}\tIndex_${cid}\n"}
        }.collect
      }
      log(s"Cells|${quadtree.getLeafZones.size}")
      Quadtree.save(quadtree, "/tmp/edgesCells.wkt")
      val MFfile = "/tmp/edgesMF.wkt"
      save(MFfile){
        maximalsRDD.mapPartitionsWithIndex{ case(cid, it) =>
          it.map{ p => s"${p.wkt}\n"}
        }.collect
      }

      checkMF(maximalsRDD)
    }

    spark.close()

    log(s"Done.|END")
  }
}
