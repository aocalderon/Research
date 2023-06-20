package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point}
import org.locationtech.jts.index.strtree._

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

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
      cached = params.cached(),
      density = params.density()
    )

    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))

    printParams(args)
    log(s"START|")

    /*** Load data ***/
    val (pointsRaw, quadtree_prime, cells_prime, tIndex1) =
      if(settings.cached){
        loadCachedData[Point]
      } else {
        loadData[Point]
      }
    val nIndex1 = cells_prime.size
    log(s"Index1|$nIndex1")
    logt(s"Index1|$tIndex1")
    settings.partitions = nIndex1

    /*** Group by Cell ***/
    val ( (stats_prime, nShuffle1), tShuffle1) = timer{
      val stats_prime = pointsRaw.mapPartitionsWithIndex{ case(cid, it) =>
        it.flatMap{ point =>
          // make a copy of envelope to avoid modification...
          val envelope = new Envelope(point.getX, point.getX, point.getY, point.getY)
          envelope.expandBy(settings.epsilon)
          quadtree_prime.findZones(new QuadRectangle(envelope)).asScala
            .map{ zone =>
              (zone.partitionId.toInt, point)
            }.toList
        }
      }.groupBy{_._1}.cache

      val nStats_prime = stats_prime.count

      (stats_prime, nStats_prime)
    }
    log(s"Shuffle1|$nShuffle1")
    logt(s"Shuffle1|$tShuffle1")


    /*** Compute statistics per Cell ***/
    val (_, tStats) = timer{
      val stats = stats_prime.map{ case(cid, it) =>
          val tree = new STRtree()
          val points = it.map(_._2).toList
          points.foreach{ point =>
            tree.insert(point.getEnvelopeInternal, point)
          }
          val nPairs = points.map{ point =>
            // make a copy of envelope to avoid modification...
            val envelope = new Envelope(point.getX, point.getX, point.getY, point.getY)
            envelope.expandBy(settings.epsilon)
            val neighbourhood = tree.query(envelope).asScala.map{_.asInstanceOf[Point]}
            val pairs = for{
              p2 <- neighbourhood
              if{
                val id1 = point.getUserData.asInstanceOf[Data].id 
                val id2 = p2.getUserData.asInstanceOf[Data].id

                id1 < id2 && point.distance(p2) < settings.epsilon_prime
              }
            } yield {
              1
            }
            pairs.size
          }.sum
          (cid, nPairs)
      }.collect

      for{
        key  <- cells_prime.keys
        stat <- stats
        if( cells_prime(key).cid == stat._1 )
          } yield {
        cells_prime(key).nPairs = stat._2
      }

      debug{
        save("/tmp/edgesCellsStats.wkt"){
          cells_prime.values.map{_.wkt + "\n"}.toList
        }
      }
    }
    logt(s"Stats|$tStats")

    /*** Repartition dense cells ***/
    val ( (quadtree, cells), tIndex2) = timer{
      if(true) {
        if(settings.dense){
          val (changes_prime, no_changes) = cells_prime.values
            .partition(_.nPairs > settings.density)
          val changes = changes_prime.flatMap(cell => createInnerGrid(cell, squared = true))
            .map{ envelope => new Cell(envelope, 0, "", true)}.toList
          val mbrs_prime = (changes ++ no_changes).zipWithIndex
            .map{ case(mbr, id) => mbr.copy(cid = id) }
          val mbrs = mbrs_prime.map{cell => cell.cid -> cell}.toMap
          val quad = new STRtree(2)
          mbrs.values.foreach{ mbr => quad.insert(mbr.mbr, mbr) }

          (quad, mbrs)
        } else {
          val quad = new STRtree(2)
          cells_prime.values.foreach{ cell =>
            quad.insert(cell.mbr, cell)
          }

          (quad, cells_prime)
        }
      } else {
        val cells = Quadtree.loadCells("/tmp/edgesCellsStats.wkt")
        val quad = new STRtree(2)
        cells_prime.values.foreach{ cell =>
          quad.insert(cell.mbr, cell)
        }

        (quad, cells)
      }
    }
    val nIndex2 = cells.size
    log(s"Index2|$nIndex2")
    logt(s"Index2|$tIndex2")
    settings.partitions = nIndex2

    debug{
      save("/tmp/edgesCells.wkt"){
        cells.values.map{ mbr => mbr.wkt + "\n"}.toList
      }
    }

    /*** Repartition data across the cluster ***/
    val ( (pointsRDD, nShuffle), tShuffle) = timer{
      val pointsRDD = pointsRaw.mapPartitions{ points =>
        points.flatMap{ point =>
          val pad = (settings.epsilon_prime * 1.5) + settings.tolerance
          val envelope = new Envelope(
            point.getX - pad,
            point.getX + pad,
            point.getY - pad,
            point.getY + pad
          )
          quadtree.query(envelope).asScala.map{ cell =>
            val cid = cell.asInstanceOf[Cell].cid

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
    log(s"Shuffle2|$nShuffle")
    logt(s"Shuffle2|$tShuffle")

    debug{
      save("/tmp/edgesPoints.wkt"){
        pointsRDD.mapPartitionsWithIndex{ case(cid, it) =>
          it.map{ p => s"${p.wkt}\tIndex_${cid}\n"}
        }.collect
      }
    }

    /*** Run BFE at each cell ***/
    val ( (maximalsRDD, nRun), tRun) = timer{
      val maximalsRDD = pointsRDD.mapPartitionsWithIndex{ case(cid, it) =>
        val cell = cells(cid)
        val points = it.toList

        val (maximals, stats) = MF_Utils.runBFEParallel(points, cell)

        debug{
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

    log(s"END|")
  }
}
