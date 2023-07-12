package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point}
import org.locationtech.jts.index.strtree.STRtree

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import sys.process._

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.Partitioner

import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.MF_Utils._

object MF {

  def run(implicit spark: SparkSession, S: Settings, G: GeometryFactory, L: Logger): RDD[Disk] = {

    /*** Load data                                         ***/
    /*** read from HDFS and retung cells and spatial index ***/
    val (pointsRaw, spatial_index, cells, tIndex1) = loadData[Point]
    val nIndex1 = cells.size
    log(s"Index1|$nIndex1")
    logt(s"Index1|$tIndex1")
    S.partitions = nIndex1


    ///////////////////// Debug
    if(S.saves){
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
          //val pad = (S.epsilon_prime * 1.5) + S.tolerance
          //val pad = (S.epsilon_prime * 1) + S.tolerance
          val pad = S.epsilon

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
      }.partitionBy{ SimplePartitioner(cells.size) }.cache
      val nPoints = points.count

      (points, nPoints)
    }
    log(s"Shuffle1|$nShuffle1")
    logt(s"Shuffle1|$tShuffle1")

    /*** Get candidates                                        ***/
    /*** early call to count pairs to decide extra partitionig ***/
    val ( (candidatesRDD, nCandidates), tCandidates ) = timer{
      val candidates = pointsRDD.mapPartitionsWithIndex{ case(cid, it) =>
        val cell   = cells(cid)
        val points = it.map(_._2).toList
        val stats  = Stats()

        getCandidatesAtCell(points, cell, stats)

      }.cache
      val nCandidates = countCandidates(candidates.map(_._2), cells)

      (candidates, nCandidates)
    }
    log(s"Candidates|$nCandidates")
    logt(s"Candidates|$tCandidates")


    ///////////////////// Debug
    if(S.saves){
      save("/tmp/edgesCandidates.wkt"){
        candidatesRDD.mapPartitionsWithIndex{ case(cid, it) =>
          it.flatMap{ case(candidatesByKey, _) =>
            candidatesByKey.flatMap{ candidateByKey =>
              val key = candidateByKey.key
              val cellId = candidateByKey.cellId
              candidateByKey.candidates.map{ candidate =>
                val wkt = candidate.center.buffer(25).toText
                s"$wkt\t$cellId\t$key\n"
              }
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


    // START MF for denser cells !!!
     
    /*** Get maximals from set of pairs,        ***/
    /*** should works after getPairsAtCell call ***/
    val ( (maximalsRDD2, nRun2), tRun2) = timer{
      val maximals1 = candidatesRDD.mapPartitionsWithIndex{ case(cid, it) =>
        if(it.isEmpty){
          Iterator.empty
        } else {
          val cell = cells(cid)
          val (candidatesByKey, stats1) = it.next()

          if(cell.nCandidates >= S.density){
            debug{ println(s"Dense cell detected: $cid")  }
            candidatesByKey.map{ candidateByKey =>
              val key = candidateByKey.key.toInt
              (key, (candidateByKey, stats1, 0))
            }.toIterator
          } else {
            val (maximals, stats2) = getMaximalsAtCell(candidatesByKey, cell, cell, stats1)
            val M = maximals.toList
            debug{ println(s"Processing cell $cid: ${M.size} maximals found.")  }
            Iterator(  (-1, (candidatesByKey.head, stats1, M.size))  )
          }
        }
      }.cache

      val grids = maximals1.map(_._1).distinct.collect.sorted.zipWithIndex.toMap
      debug{
        println(
          s"KEYs: "+grids.toList.sortBy(_._2).map{case(key, value) => s"$key $value"}.mkString(";")
        )
        val partial = maximals1.map{_._2._3}.sum
        println(s"Partial maximals found: ${partial}")
      }

      val maximals2 = maximals1.filter(_._1 >= 0).partitionBy{ MapPartitioner(grids) }.cache
      maximals2.count

      ///////////////////////////////////////////////////////////////////////////////////////////////
      val x = maximals2.mapPartitionsWithIndex{ case(cid, it) =>
        if(it.isEmpty){
          Iterator.empty
        } else {
          val (_, (candidatesByKey, stats1, _)) = it.next()
          val cellId = candidatesByKey.cellId
          val key    = candidatesByKey.key.toInt

          println(s"$cellId\t$key")

          //val cell = cells(cellId)
          //val boundary = new Envelope(cell.mbr)
          //boundary.expandBy(S.expansion)
          //val inner_cells = recreateInnerGrid(cell.copy(mbr = boundary), expansion = true)
          //val inner_cell  = inner_cells.filter(_.cid == key).head

          Iterator.empty
        }
      }
      x.count
      ///////////////////////////////////////////////////////////////////////////////////////////////

      (maximals2, maximals2.count)

      /*

       maximals2.mapPartitionsWithIndex{ case(cid, it) =>
        if(it.isEmpty){
          Iterator.empty
        } else {
          val (_, (pairsByKey, stats1)) = it.next()
          val cellId = pairsByKey.cellId
          val key    = pairsByKey.key.toInt

          val cell = cells(cellId)
          val boundary = new Envelope(cell.mbr)
          boundary.expandBy(S.expansion)
          val inner_cells = recreateInnerGrid(cell.copy(mbr = boundary), expansion = true)
          val inner_cell  = inner_cells.filter(_.cid == key).head
          if(S.saves){
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

      */ // END MF for denser cell !!!
    }
    log(s"Run2|$nRun2")
    logt(s"Run2|$tRun2")


    ///////////////////// Debug
    if(S.saves){
      //save("/tmp/edgesMF2.wkt"){
        //maximalsRDD2.mapPartitionsWithIndex{ case(cid, it) =>
        //  it.map{ p => s"${p.wkt}\t$cid\n"}
        //}.collect.sorted
      //}
    }
    ///////////////////// Debug


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
    if(S.saves){
      save("/tmp/edgesMF.wkt"){
        maximalsRDD.mapPartitionsWithIndex{ case(cid, it) =>
          it.map{ p => s"${p.wkt}\t$cid\n"}
        }.collect.sorted
      }
    }
    ///////////////////// Debug

    maximalsRDD
  }

  def main(args: Array[String]): Unit = {
    implicit val params = new BFEParams(args)

    implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

    println(s"NAME       = ${appName}")
    implicit val spark = SparkSession.builder()
      .config("spark.testing.memory", "2147480000")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .master(params.master())
      .appName(appName).getOrCreate()
    import spark.implicits._

    implicit var S = Settings(
      dataset = params.dataset(),
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
      density = params.density(),
      appId = spark.sparkContext.applicationId
    )

    implicit val geofactory = new GeometryFactory(new PrecisionModel(S.scale))

    printParams(args)
    log(s"START|")

    val begin = params.begin()
    val pointsByTime_prime = f"hdfs dfs -ls ${params.dataset()}/part-${begin}%05d*".lineStream_!
    val pointsByTime = pointsByTime_prime.head.split(" ").last
    val path_tail = pointsByTime.split(f"-${begin}%05d-")(1)

    val end   = params.end()
    ( {if(begin < 0) 0 else begin} to {if(end < begin) begin else end} ).foreach{ time =>

      val current_dataset = f"${params.dataset()}/part-${time}%05d-${path_tail}"
      println(current_dataset)
      S = S.copy(dataset = current_dataset)
      val maximalsRDD = run

      if(S.tester){
        val points  = readSTPointsFromFile(S.dataset)
        val m1      = maximalsRDD.collect.toList
        val (m2, _) = BFE.run(points)

        diff(testing = m1, validation = m2, points)
      }
    }

    spark.close()

    log(s"END|")
  }
}
