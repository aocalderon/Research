package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{GeometryFactory, Envelope, Coordinate, Point}

import org.apache.spark.sql.SparkSession
import org.apache.spark.TaskContext
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._

import sys.process._
import org.slf4j.Logger
import archery._

import quadtree._
import Utils._

object MF_Utils {

  def runBFEParallel(points_prime: List[STPoint], cell: Cell)
    (implicit S: Settings, geofactory: GeometryFactory, logger: Logger)
      : (Iterator[Disk], Stats) = {

    if(points_prime.isEmpty){
      (List.empty[Disk].toIterator, Stats())
    } else {
      if(cell.dense){
        val cid = TaskContext.getPartitionId
        val stats = Stats()
        var Maximals: RTree[Disk] = RTree()

        val Pr = points_prime.filter(point => cell.mbr.contains(point.getCoord))
        val Ps = points_prime

        var tCenters = 0.0
        var tCandidates = 0.0
        var tMaximals = 0.0

        val (_, tPairs) = timer{
          if(Ps.size >= S.mu){
            for{ pr <- Pr }{
              val H = pr.getNeighborhood(Ps) // get range around pr in Ps...

              if(H.size >= S.mu){ // if range as enough points...

                for{
                  ps <- H if{ pr.oid < ps.oid }
                } yield {
                  // a valid pair...
                  stats.nPairs += 1

                  val (disks, tC) = timer{
                    // finding centers for each pair...
                    val centers = calculateCenterCoordinates(pr.point, ps.point)
                    // querying points around each center...
                    centers.map{ center =>
                      getPointsAroundCenter(center, Ps)
                    }
                  }
                  stats.nCenters += 2
                  tCenters += tC

                  val (candidates, tD) = timer{
                    // getting candidate disks...
                    disks.filter(_.count >= S.mu)
                  }
                  stats.nCandidates += candidates.size
                  tCandidates += tD

                  val (_, tM) = timer{
                    // cheking if a candidate is not a subset and adding to maximals...
                    candidates.foreach{ candidate =>
                      Maximals = insertMaximalParallel(Maximals, candidate, cell)//
                    }
                  }
                  tMaximals += tM
                }
              }
            }
          }
        }
        stats.tCenters += tCenters
        stats.tCandidates += tCandidates
        stats.tMaximals += tMaximals
        stats.tPairs += tPairs - (tCenters + tCandidates + tMaximals)

        val M = Maximals.entries.toList.map(_.value).filter{ maximal => cell.contains(maximal) }
        stats.nMaximals = M.size

        debug{
          save(s"/tmp/edgesMaximals${cid}.wkt"){ M.map{_.wkt + "\n"} }
        }

        (M.toIterator, stats)
      } else {
        runBFEParallelSimple(points_prime, cell)
      }
    } 
  }

  def runBFEParallelSimple(points_prime: List[STPoint], cell: Cell)
    (implicit settings: Settings, geofactory: GeometryFactory, logger: Logger)
      : (Iterator[Disk], Stats) = {

    val cid = TaskContext.getPartitionId
    val stats = Stats()
    var Maximals: RTree[Disk] = RTree()

    val (points, tCounts) = timer{
      computeCounts(points_prime)
    }
    stats.nPoints = points.size
    stats.tCounts = tCounts

    val (grid, tGrid) = timer{
      val G = Grid(points)
      G.buildGrid
      G
    }
    stats.tGrid = tGrid

    debug{
      log(s"GridSize=${grid.index.size}")
      save(s"/tmp/edgesPoints${cid}.wkt"){ grid.pointsToText }
      save(s"/tmp/P${cid}.tsv"){ points_prime.map{_.toString + "\n"} }
      save(s"/tmp/edgesGrid${cid}.wkt"){ grid.wkt() }
    }

    val the_key = -1 // for debugging...

    // for each non-empty cell...
    grid.index.keys.foreach{ key =>
      val ( (_Pr, _Ps), tRead ) = timer{
        val (i, j) = decode(key) // position (i, j) for current cell...
        val Pr = grid.index(key) // getting points in current cell...

        val indices = List( // computing positions (i, j) around current cell...
          (i-1, j+1),(i, j+1),(i+1, j+1),
          (i-1, j)  ,(i, j)  ,(i+1, j),
          (i-1, j-1),(i, j-1),(i+1, j-1)
        ).filter(_._1 >= 0).filter(_._2 >= 0) // just keep positive (i, j)...

        val Ps = indices.flatMap{ case(i, j) => // getting points around current cell...
          val key = encode(i, j)
          if(grid.index.keySet.contains(key))
            grid.index(key)
          else
            List.empty[STPoint]
        }
        (Pr, Ps)
      }
      stats.tRead += tRead
      val Pr = _Pr
      val Ps = _Ps

      debug{
        if(key == the_key) println(s"Key: ${key} Ps.size=${Ps.size}")
      }

      var tCenters = 0.0
      var tCandidates = 0.0
      var tMaximals = 0.0

      val (_, tPairs) = timer{
        if(Ps.size >= settings.mu){
          for{ pr <- Pr }{
            val H = pr.getNeighborhood(Ps) // get range around pr in Ps...

            debug{
              if(key == the_key) println(s"Key=${key}\t${pr.oid}\tH.size=${H.size}")
            }

            if(H.size >= settings.mu){ // if range as enough points...

              for{
                ps <- H if{ pr.oid < ps.oid }
              } yield {
                // a valid pair...
                stats.nPairs += 1

                val (disks, tC) = timer{
                  // finding centers for each pair...
                  val centers = calculateCenterCoordinates(pr.point, ps.point)
                  // querying points around each center...
                  centers.map{ center =>
                    getPointsAroundCenter(center, Ps)
                  }
                }
                stats.nCenters += 2
                tCenters += tC

                val (candidates, tD) = timer{
                  // getting candidate disks...
                  disks.filter(_.count >= settings.mu)
                }
                stats.nCandidates += candidates.size
                tCandidates += tD

                val (_, tM) = timer{
                  // cheking if a candidate is not a subset and adding to maximals...
                  candidates.foreach{ candidate =>
                    Maximals = insertMaximalParallel(Maximals, candidate, cell)//
                  }
                }
                tMaximals += tM
              }
            }
          }
        }
      }
      stats.tCenters += tCenters
      stats.tCandidates += tCandidates
      stats.tMaximals += tMaximals
      stats.tPairs += tPairs - (tCenters + tCandidates + tMaximals)
    }

    val M = Maximals.entries.toList.map(_.value).filter{ maximal => cell.contains(maximal) }
    stats.nMaximals = M.size

    debug{
      save(s"/tmp/edgesMaximals${cid}.wkt"){ M.map{_.wkt + "\n"} }
    }

    (M.toIterator, stats)
  }

  def createInnerGrid(cell: Cell, squared: Boolean = true)
    (implicit S: Settings, geofactory: GeometryFactory): List[Envelope] = {

    val mbr = cell.mbr

    val (_X, _Y, m, n) = if(squared){
      val n = math.ceil(mbr.getWidth / S.epsilon_prime)
      val X = (mbr.getMinX until mbr.getMaxX by S.epsilon_prime).toList :+ mbr.getMaxX
      val m = math.ceil(mbr.getHeight / S.epsilon_prime)
      val Y = (mbr.getMinY until mbr.getMaxY by S.epsilon_prime).toList :+ mbr.getMaxY
      (X, Y, m, n)
    } else {
      val n = math.floor(mbr.getWidth / S.epsilon_prime)
      val width = mbr.getWidth / n
      val X = (mbr.getMinX until mbr.getMaxX by width).toList :+ mbr.getMaxX // ensure maxX...
      val m = math.floor(mbr.getHeight / S.epsilon_prime)
      val height = mbr.getHeight / m
      val Y = (mbr.getMinY until mbr.getMaxY by height).toList :+ mbr.getMaxY // ensure maxY...
        (X, Y, m, n)
    }

    (for{
      i <- 0 until n.toInt
      j <- 0 until m.toInt
    } yield {
      new Envelope( _X(i), _X(i + 1), _Y(j), _Y(j + 1) )
    }).toList
  }

  def loadCachedData[T]
    (implicit spark: SparkSession, S: Settings, geofactory: GeometryFactory, logger: Logger)
      : (RDD[Point], StandardQuadTree[T], Map[Int, Cell], Double) = {

    val ((quadtree, cells), tIndex) = timer{
      val home = System.getenv("HOME")
      val git_path = "/Research/local_path"
      val filename = s"${home}/${git_path}/${getHDFSPath}/quadtree${S.dataset}.wkt"
      Quadtree.loadQuadtreeAndCells[T](filename)
    }

    val ( (pointsRaw, nRead), tRead) = timer{
      val pointsRaw = spark.read
        .option("delimiter", "\t")
        .option("header", false)
        .textFile(S.input).rdd
        .map { line =>
          val arr = line.split("\t")
          val i = arr(0).toInt
          val x = arr(1).toDouble
          val y = arr(2).toDouble
          val t = arr(3).toInt
          val c = arr(4).toInt
          val point = geofactory.createPoint(new Coordinate(x, y))
          point.setUserData(Data(i, t))
          (c, point)
        }.partitionBy(new Partitioner {
          def numPartitions: Int = cells.size
          def getPartition(key: Any): Int = key.asInstanceOf[Int]
        }).cache
        .map(_._2).cache
      val nRead = pointsRaw.count
      (pointsRaw, nRead)
    }
    log(s"Read|$nRead")
    logt(s"Read|$tRead")

    (pointsRaw, quadtree, cells, tIndex)
  }

  def loadData[T]
    (implicit spark: SparkSession, S: Settings, geofactory: GeometryFactory, logger: Logger)
      : (RDD[Point], StandardQuadTree[Point], Map[Int, Cell], Double) = {

    val ( (pointsRaw, nRead), tRead) = timer{
      val pointsRaw = spark.read
        .option("delimiter", "\t")
        .option("header", false)
        .textFile(S.input).rdd
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
      val quadtree = Quadtree.getQuadtreeFromPoints(pointsRaw)
      val cells = quadtree.getLeafZones.asScala.map{ leaf =>
        val cid = leaf.partitionId.toInt
        val lin = leaf.lineage
        val env = leaf.getEnvelope

        cid -> Cell(env, cid, lin)
      }.toMap

      (quadtree, cells)
    }

    (pointsRaw, quadtree, cells, tIndex)
  }

  def getHDFSPath(implicit S: Settings): String = S.input.split("/").reverse.tail.reverse.mkString("/")

  def getPaths(implicit S: Settings): (String, String) = {
    val home = System.getenv("HOME")
    val git_path = "/Research/local_path"
    val dataset = S.dataset

    val hdfs_path = s"${S.output}/${dataset}"
    //s"hdfs dfs -mkdir $hdfs_path".!

    val fs_path = s"${home}${git_path}/${S.output}"
    //s"mkdir -p $fs_path".!

    (hdfs_path, fs_path)
  }

  def getHDFSandFSnames(prefix: String = "", ext: String = "wkt")
    (implicit S: Settings): (String, String) = {

    val (hdfs_path, fs_path) = getPaths
    val fs   = s"${fs_path}/${prefix}${S.dataset}.${ext}"
    val hdfs = s"${hdfs_path}"

    (hdfs, fs)
  }
}
