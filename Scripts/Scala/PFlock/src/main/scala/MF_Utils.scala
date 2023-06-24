package edu.ucr.dblab.pflock

import archery._
import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.quadtree._
import org.apache.spark.{Partitioner, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.{Coordinate, Envelope, GeometryFactory, Point}
import org.slf4j.Logger

import edu.ucr.dblab.pflock.spmf.{DoubleArray, KDTree}

import scala.collection.JavaConverters._
import scala.util.control.Breaks._

object MF_Utils {

  def runBFEParallel(points_prime: List[STPoint], cell: Cell)
    (implicit S: Settings, geofactory: GeometryFactory, logger: Logger)
      : (Iterator[Disk], Stats) = {

    if(points_prime.isEmpty){
      (List.empty[Disk].toIterator, Stats())
    } else {
      if(cell.dense){
        runBFEinParallel(points_prime, cell)
      } else {
        runBFEinParallel(points_prime, cell)
      }
    } 
  }

  case class KDPoint(p: STPoint) extends DoubleArray(p.point)
  def runDenseBFEinParallel(points_prime: List[STPoint], cell: Cell)
    (implicit S: Settings, G: GeometryFactory, L: Logger): (Iterator[Disk], Stats) = {

    val cid = TaskContext.getPartitionId
    var Maximals: RTree[Disk] = RTree()
    val stats = Stats()

    val ( (kdtree, points), tGrid) = timer{
      val kdtree = new KDTree()
      val vectors = points_prime.map{ point => KDPoint(point).asInstanceOf[DoubleArray] }.asJava
      kdtree.buildtree(vectors)

      val boundary = new Envelope(cell.mbr)
      boundary.expandBy(S.epsilon)
      val points = points_prime.filter{ p => boundary.contains(p.X, p.Y) }

      (kdtree, points)
    }
    stats.tGrid = tGrid
    stats.nPoints = points.size

    var counter = 0
    points.foreach{ point =>
      var tCenters = 0.0
      var tCandidates = 0.0
      var tMaximals = 0.0
      val (tCDM, tPairs) = timer{
        val Pr = KDPoint(point)
        val H = kdtree.pointsWithinRadiusOf(Pr, S.epsilon).asScala
          .map{_.asInstanceOf[KDPoint].p}.toList
        // if range as enough points...
        if(H.size >= S.mu) {
          H.filter(_.oid < point.oid).map{ ps =>
            // a valid pair...
            stats.nPairs += 1

            val (disks, tC) = timer{
              // finding centers for each pair...
              val centers = calculateCenterCoordinates(point.point, ps.point)
              // querying points around each center...
              centers.map{ center =>
                val query =new  DoubleArray(center)
                val hood = kdtree.pointsWithinRadiusOf(query, S.r).asScala
                  .map{_.point.getUserData.asInstanceOf[Data].id}.toList
                Disk(center, hood)
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
                Maximals = insertMaximalParallel(Maximals, candidate, cell)
              }
            }
            tMaximals += tM
            (tC + tD + tM)
          }.sum
        } else {
          0.0
        }
      }
      stats.tPairs += tPairs - tCDM
      stats.tCenters += tCenters
      stats.tCandidates += tCandidates
      stats.tMaximals += tMaximals
    }

    val M = Maximals.entries.toList.map(_.value).filter{ maximal => cell.contains(maximal) }
    stats.nMaximals = M.size

    debug{
      save(s"/tmp/edgesMaximals${cid}.wkt"){ M.map{_.wkt + "\n"} }
    }

    (M.toIterator, stats)
  }

  def runBFEinParallel(points_prime: List[STPoint], cell: Cell)
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

  def insertMaximalParallel(maximals: archery.RTree[Disk], candidate: Disk, cell: Cell)
    (implicit settings: Settings): archery.RTree[Disk] = {

    val cid = TaskContext.getPartitionId
    if(maximals.entries.size == 0){
      // candidate is the first entry so we insert it...
      val center = archery.Point(candidate.X, candidate.Y)
      val toInsert = Entry(center, candidate)
      maximals.insert(toInsert)
    } else {
      // we query the tree to retrieve maximals around the current candidate...
      val maximals_prime = maximals.search(candidate.bbox(settings.epsilon.toFloat)).map(_.value)

      // we check if candidate is subset of any current maximal...
      if( maximals_prime.exists( maximal => candidate.isSubsetOf(maximal) ) ){
        // if so, we have to check if candidate is equal to that maximal...
        // (if candidate is subset, it could be equal to one and only one maximal)...
        // (if not, the tree has duplicates)...

        // we find for a maximal with equal point ids (pids) to current candidate...
        val maximal_prime = maximals_prime.find(_.equals(candidate))
        maximal_prime match {
          // If so...
          case Some(maximal) => { // maximal pids == current pids...
            // to be deterministic, we only replace if new candidate is most left-down disk...         
            if(candidate < maximal){ // it is implemented in Disk class...
              maximals
                .remove(Entry(archery.Point(maximal.X, maximal.Y), maximal))
                .insert(Entry(archery.Point(candidate.X, candidate.Y), candidate))
            } else {
              // the current maximal is still the most left-down disk...
              // so we keep the tree without changes...
              maximals
            }
          }
          // None means they are not equal (candidate is a truly subset)...
          // so we keep the tree without changes...
          case None => maximals
        }
      } else {
        // we check if candidate is superset of one or more maximals...
        if( maximals_prime.exists( maximal => maximal.isSubsetOf(candidate) ) ){
          // we find for a maximal with equal point ids (pids) to current candidate...
          val maximal_prime = maximals_prime.find(_.equals(candidate))
          maximal_prime match {
            // if so...
            case Some(maximal) => { // maximal pids == current pids...
              // to be deterministic, we only replace if new candidate is most left-down point...
              if(candidate < maximal){ // it is implemented in Disk class...
                maximals
                  .remove(Entry(archery.Point(maximal.X, maximal.Y), maximal))
                  .insert(Entry(archery.Point(candidate.X, candidate.Y), candidate))
              } else {
                // the current maximal is still the most left-down disk...
                // so we keep the tree without changes...
                maximals
              }
            }
            // None means there is not any equal, just subset(s) so
            // we remove subset(s) and insert new candidate...
            case None => {
              // collect a list of one or more maximal subsets...
              val toRemove = maximals_prime.filter( maximal => maximal.isSubsetOf(candidate) )
                .map{ maximal =>
                  val center = archery.Point(maximal.X, maximal.Y)
                  Entry(center, maximal)
                }
              val center = archery.Point(candidate.X, candidate.Y)
              val toInsert = Entry(center, candidate)
              // remove subset(s) and insert new candidate...
              maximals.removeAll(toRemove).insert(toInsert)
           }
          }
        } else {
          // candidate is neither subset or superset (ergo, not equal)...
          // so we insert it...
          val center = archery.Point(candidate.X, candidate.Y)
          val toInsert = Entry(center, candidate)
          maximals.insert(toInsert)
        }
      }
    }
  }

  def insertMaximalParallel2(maximals: archery.RTree[Disk], candidate: Disk, cell: Cell,
    counter: Int = 0) (implicit S: Settings, L: Logger): archery.RTree[Disk] = {

    val cid = TaskContext.getPartitionId
    if(maximals.size == 0){
      val (firstM, tM) = timer{
        // candidate is the first entry so we insert it...
        val center = archery.Point(candidate.X, candidate.Y)
        val toInsert = Entry(center, candidate)
        maximals.insert(toInsert)
      }
      debug{
        log( s"${counter}|MAXIMALS| sizeM|${maximals.size}")
        logt(s"${counter}|MAXIMALS|firstM|$tM")
      }

      firstM
    } else {
      // we query the tree to retrieve maximals around the current candidate...
      val (maximals_prime, tS1) = timer{
        maximals.search(candidate.bbox(S.epsilon.toFloat)).map(_.value)
      }
      debug{
        log( s"${counter}|MAXIMALS| sizeM|${maximals.size}")
        log( s"${counter}|MAXIMALS| sizeH|${maximals_prime.size}")
        logt(s"${counter}|MAXIMALS|Search|$tS1")
      }

      var Mx = archery.RTree[Disk]()
      var flag = 0
      var t = 0.0
      var i = 0

      for( maximal <- maximals_prime if flag == 0) {
        // println(s"Maximal: ${maximal.pidsText}")
        val (_M, tM) = timer {
          if( maximal.distance(candidate) > S.epsilon ) { // M disjoint C
            // refine stage after filter the tree...
            // maximal and candidate disjoint (pids cannot intersect), we can continue...
          } else if( maximal.equals(candidate) ) { // M equal C
            breakable{
              flag = 1 
              // to be deterministic, we only replace if new candidate is most left-down disk...
              Mx = if(candidate < maximal){ // it is implemented in Disk class...
                maximals
                  .remove(Entry(archery.Point(maximal.X, maximal.Y), maximal))
                  .insert(Entry(archery.Point(candidate.X, candidate.Y), candidate))
              } else {
                // the current maximal is still the most left-down disk...
                // so we keep the tree without changes...
                maximals
              }
              break
            }
          } else if( maximal.isSubsetOf(candidate) ) { // M subset C
            breakable { 
              flag = 2
              // collect a list of one or more maximal subsets...
              val toRemove = maximals_prime.filter( maximal => maximal.isSubsetOf(candidate) )
                .map{ maximal =>
                  val center = archery.Point(maximal.X, maximal.Y)
                  Entry(center, maximal)
                }
              val center = archery.Point(candidate.X, candidate.Y)
              val toInsert = Entry(center, candidate)
              // remove subset(s) and insert new candidate...
              Mx = maximals.removeAll(toRemove).insert(toInsert)
              break
            }
          } else if( candidate.isSubsetOf(maximal) ) { // C subset M
            breakable { 
              flag = 3 
              Mx = maximals
            }
          } else {
            // We check the three alternatives and maximal and candidate are different,
            // we can continue...
          }
        } // timer

        t += tM
        i = i + 1
      } // for

      val tag = flag match{
        case 0 => "C d M"
        case 1 => "C e M"
        case 2 => "M s C"
        case 3 => "C s M"
      }
      debug{
        log( s"${counter}|MAXIMALS|hitPos|${i}")
        logt(s"${counter}|MAXIMALS| $tag|$t")
      }

      if( flag == 0 ){
        // We iterate over all the neighborhood and candidate is different to all of them...
        // We add candidate to maximals...
        val (_Mx, tI) = timer{
         val center = archery.Point(candidate.X, candidate.Y)
         val toInsert = Entry(center, candidate)
          maximals.insert(toInsert)
        }

        debug{
          log( s"${counter}|MAXIMALS|finalM|${_Mx.size}")
          logt(s"${counter}|MAXIMALS|finalI|$tI")
        }

        _Mx
      } else {
        // Candidate was a subset, superset or equal and it was already handle it...
        // We return the new tree...
        debug{
          log( s"${counter}|MAXIMALS|finalM|${Mx.size}")
        }
        
        Mx
      }
    } // if first time...
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
      val cells = quadtree.getLeafZones.asScala.map{ leaf: QuadRectangle =>
        val cid = leaf.partitionId.toInt
        val lin = leaf.lineage
        val env = leaf.getEnvelope

        (cid, Cell(env, cid, lin))
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
