package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point}
import org.locationtech.jts.index.strtree.STRtree

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.Partitioner

import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.MF_Utils._

object MF {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params = new BFEParams(args)

    implicit var settings = Settings(
      input = params.input(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      capacity = params.capacity(),
      fraction = params.fraction(),
      tolerance = params.tolerance(),
      tag = params.tag(),
      debug = params.debug(),
      cached = params.cached(),
      tester = params.tester(),
      saves = params.saves(),
      density = params.density()
    )

    println(s"NAME       = ${settings.appName}")
    implicit val spark = SparkSession.builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .appName(settings.appName).getOrCreate()
    import spark.implicits._

    settings.appId = spark.sparkContext.applicationId
    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))

    printParams(args)
    log(s"START|")

    /*** Load data                                         ***/
    /*** read from HDFS and retung cells and spatial index ***/
    val (pointsRaw, spatial_index, cells, tIndex1) = loadData[Point]
    val nIndex1 = cells.size
    log(s"Index1|$nIndex1")
    logt(s"Index1|$tIndex1")
    settings.partitions = nIndex1


    ///////////////////// Debug
    if(settings.saves){
      save(s"/tmp/edgesCells.wkt"){
        cells.values.map{ mbr => mbr.wkt + "\n"}.toList
      }
    }
    ///////////////////// Debug


    /*** Repartition data across the cluster ***/
    /*** Partition by Cell ID                ***/
    val ( (pointsRDD, nShuffle1), tShuffle1) = timer{
      val points = pointsRaw.mapPartitionsWithIndex{ case(cid, it) =>
        it.flatMap{ point =>
          // computing a pad for the expansion area...
          //val pad = (settings.epsilon_prime * 1.5) + settings.tolerance
          val pad = (settings.epsilon_prime * 1) + settings.tolerance

          // make a copy of envelope to avoid modification...
          val envelope = new Envelope(
            point.getX - pad,
            point.getX + pad,
            point.getY - pad,
            point.getY + pad
          )
          spatial_index.query(envelope).asScala
            .map{ cell_prime =>
              val cell = cell_prime.asInstanceOf[Cell]
              (cell.cid, STPoint(point))
            }.toList
        }
      }.partitionBy{
        new Partitioner {
          def numPartitions: Int = cells.size
          def getPartition(key: Any): Int = key.asInstanceOf[Int]
        }
      }.cache
      val nPoints = points.count

      (points, nPoints)
    }
    log(s"Shuffle1|$nShuffle1")
    logt(s"Shuffle1|$tShuffle1")

/*

    /*** Get set of pairs                                      ***/
    /*** early call to count pairs to decide extra partitionig ***/
    val ( (pairsRDD, nPairs), tPairs ) = timer{
      val pairs = pointsRDD.mapPartitionsWithIndex{ case(cid, it) =>
        val cell   = cells(cid)
        val points = it.map(_._2).toList
        val stats  = Stats()

        getPairsAtCell(points, cell, stats)

      }.cache
      val nPairs = countPairs(pairs.map(_._2), cells)

      (pairs, nPairs)
    }
    log(s"Pairs|$nPairs")
    logt(s"Pairs|$tPairs")


    ///////////////////// Debug
    if(settings.saves){
      save("/tmp/edgesPairs.wkt"){
        pairsRDD.mapPartitionsWithIndex{ case(cid, it) =>
          it.flatMap{ case(pairsByKey, _) =>
            pairsByKey.flatMap{ pairByKey =>
              val key = pairByKey.key
              val cellId = pairByKey.cellId
              pairByKey.pairs.map(_.wkt + s"\t$cellId\t$key\n")
            }
          }
        }.collect
      }

      save("/tmp/edgesCellsStats.wkt"){
        cells.values.map{ cell =>
          cell.wkt + "\n"
        }.toList
      }
    }
    ///////////////////// Debug

  */

    /*

    /*** Get maximals from set of pairs,        ***/
    /*** should works after getPairsAtCell call ***/
    val ( (maximalsRDD2, nRun2), tRun2) = timer{
      val maximals1 = pairsRDD.mapPartitionsWithIndex{ case(cid, it) =>
        if(it.isEmpty){
          Iterator.empty
        } else {
          val cell = cells(cid)
          val (pairsByKey, stats1) = it.next()

          if(cell.nPairs >= settings.density){
            pairsByKey.map{ pairByKey =>
              val key = pairByKey.key
              (key, (pairByKey, stats1))
            }.toIterator
          } else {
            val (maximals, stats2) = getMaximalsAtCell(pairsByKey, cell, cell, stats1)
            debug{
              //stats2.print()
            }
            val M = maximals.toList
            save(s"/tmp/edgesPMF${cid}"){
              M.map{_.wkt + "\n"}
            }
            log(s"MF|${M.size}")
            Iterator.empty
          }
        }
      }.cache
      val grids = maximals1.map(_._1).distinct.collect.sorted.zipWithIndex.toMap
      debug{
        println(
          s"KEY: "+grids.toList.sortBy(_._2).map{case(key, value) => s"$key $value"}.mkString(";")
        )
      }
      val grids_reverse = grids.map(_.swap)
      val maximals2 = maximals1.partitionBy{
        new Partitioner {
          def numPartitions: Int = grids.size
          def getPartition(key: Any): Int = grids(key.asInstanceOf[Long])
        }
      }.mapPartitionsWithIndex{ case(cid, it) =>
        if(it.isEmpty){
          Iterator.empty
        } else {
          val (_, (pairsByKey, stats1)) = it.next()
          val cellId = pairsByKey.cellId
          val key    = pairsByKey.key.toInt

          val cell = cells(cellId)
          val boundary = new Envelope(cell.mbr)
          boundary.expandBy(settings.expansion)
          val inner_cells = recreateInnerGrid(cell.copy(mbr = boundary), expansion = true)
          val inner_cell  = inner_cells.filter(_.cid == key).head
          if(settings.saves){
            save(s"/tmp/edgesIC${key}.wkt"){
              inner_cells.filter(_.cid == key).map{ inner_cell =>
                s"${inner_cell.wkt}\t$key\n"
              }
            }
            save(s"/tmp/edgesPT${key}.wkt"){
              pairsByKey.Ps.map{_.wkt + "\n"}
            }
          }
          //val (maximals, stats2) = getMaximalsAtCell(List(pairsByKey), cell, inner_cell, stats1, 2)
          val Ps = pairsByKey.Ps
          val (maximals, stats2) = BFE.run(Ps)
          debug{
            //stats2.print()
          }
          maximals.toIterator.filter(m => inner_cell.contains(m)).filter(m => cell.contains(m))
        }
      }.cache
      val nRun = maximals2.count
      (maximals2, nRun)
    }
    log(s"Run2|$nRun2")
    logt(s"Run2|$tRun2")


    ///////////////////// Debug
    if(settings.saves){
      save("/tmp/edgesMF2.wkt"){
        maximalsRDD2.mapPartitionsWithIndex{ case(cid, it) =>
          it.map{ p => s"${p.wkt}\t$cid\n"}
        }.collect.sorted
      }
    }
     ///////////////////// Debug

     */


    /*** Run BFE at each cell ***/
    val ( (maximalsRDD, nRun), tRun) = timer{
      val maximalsRDD = pointsRDD.mapPartitionsWithIndex{ case(cid, it) =>
        val cell = cells(cid)
        val points = it.map(_._2)toList

        val (maximals, stats) = MF_Utils.runBFEParallel(points, cell)

        debug{
          //stats.print()
        }

        maximals
      }.cache
      
      val nRun = maximalsRDD.count
      (maximalsRDD, nRun)
    }
    log(s"Run|$nRun")
    logt(s"Run|$tRun")


    ///////////////////// Debug
    if(settings.saves){
      save("/tmp/edgesMF.wkt"){
        maximalsRDD.mapPartitionsWithIndex{ case(cid, it) =>
          it.map{ p => s"${p.wkt}\t$cid\n"}
        }.collect.sorted
      }
    }
    ///////////////////// Debug


    ///////////////////// Validation
    if(settings.tester){
      //val m1 = maximalsRDD2.collect.toList
      val points  = pointsRDD.map(_._2).collect.toList
      val m1      = maximalsRDD.collect.toList
      val (m2, _) = BFE.run(points)

      diff(testing = m1, validation = m2, points)
    }
    ///////////////////// Validation

    spark.close()

    log(s"END|")
  }
}
