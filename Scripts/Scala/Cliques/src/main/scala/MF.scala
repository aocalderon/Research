package edu.ucr.dblab.pflock

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Envelope, Coordinate, Point}

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.Partitioner

import edu.ucr.dblab.pflock.quadtree._
import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.MF_Utils._

object MF {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params = new BFEParams(args)

    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .appName("MF").getOrCreate()
    import spark.implicits._

    implicit var settings = Settings(
      input = params.input(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = "PFlocks",
      capacity = params.capacity(),
      fraction = params.fraction(),
      appId = spark.sparkContext.applicationId,
      tolerance = params.tolerance(),
      tag = params.tag(),
      debug = params.debug(),
      cached = params.cached()
    )

    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))

    printParams(args)

    val (pointsRaw, quadtree, cells) = if(settings.cached){
      loadCachedData
    } else {
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

      val ((quadtree, cells), tIndex) = timer{
        val quadtree = Quadtree.getQuadtreeFromPoints(pointsRaw)
        val cells = quadtree.getLeafZones.asScala.map{ leaf =>
          val cid = leaf.partitionId.toInt
          val lin = leaf.lineage
          val env = leaf.getEnvelope

          cid -> Cell(env, cid, lin)
        }.toMap
        quadtree.dropElements()
        (quadtree, cells)
      }
      val nIndex = cells.size
      log(s"Index|$nIndex")
      logt(s"Index|$tIndex")

      (pointsRaw, quadtree, cells)
    }
    settings.partitions = cells.size

    debug{ Quadtree.save(quadtree, "/tmp/edgesCells.wkt") }

    val ( (pointsRDD, nShuffle), tShuffle) = timer{
      val pointsRDD = pointsRaw.mapPartitions{ points =>
        points.flatMap{ point =>
          val envelope = point.getEnvelopeInternal
          envelope.expandBy(settings.epsilon * 1.5)
          val rectangle = new QuadRectangle(envelope)
          quadtree.findZones(rectangle).asScala.map{ cell =>
            val cid = cell.partitionId

            (cid, STPoint(point, cid))
          }
        }
      }.partitionBy(new Partitioner {
        def numPartitions: Int = cells.size
        def getPartition(key: Any): Int = key.asInstanceOf[Int]
      }).cache
        .map(_._2).cache
      val nShuffle = pointsRDD.count
      (pointsRDD, nShuffle)
    }
    log(s"Shuffle|$nShuffle")
    logt(s"Shuffle|$tShuffle")

    debug{
      save("/tmp/edgesPoints.wkt"){
        pointsRDD.mapPartitionsWithIndex{ case(cid, it) =>
          it.map{ p => s"${p.wkt}\tIndex_${cid}\n"}
        }.collect
      }
    }

    val ( (maximalsRDD, nRun), tRun) = timer{
      val maximalsRDD = pointsRDD.mapPartitionsWithIndex{ case(cid, it) =>
        val cell = cells(cid)
        val points = it.toList
        val (maximals, stats) = BFE.runParallel(points, cell)
        debug{
          println(s"CID=$cid")
          stats.print()
        }

        maximals
      }.cache
      val nRun = maximalsRDD.count
      (maximalsRDD, nRun)
    }
    log(s"Run|$nRun")
    logt(s"Run|$tRun")

    debug{
      save("/tmp/edgesMF.wkt"){
        maximalsRDD.mapPartitionsWithIndex{ case(cid, it) =>
          it.map{ p => s"${p.wkt}\t$cid\n"}
        }.collect
      }
    }
    
    val maximalsMF = maximalsRDD.collect.toList
    spark.close()

    debug{
      //settings = settings.copy(method = "BFE")
      //checkMF(maximalsMF)
    }

    log(s"Done.|END")
  }
}
