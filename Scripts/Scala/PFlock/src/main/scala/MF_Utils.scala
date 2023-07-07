package edu.ucr.dblab.pflock

import org.apache.spark.{Partitioner, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

import org.locationtech.jts.geom.{Coordinate, Envelope, GeometryFactory, Point, LineString}
import org.locationtech.jts.index.strtree.STRtree

import scala.collection.JavaConverters._
import sys.process._

import org.slf4j.Logger
import archery._

import edu.ucr.dblab.pflock.spmf.{DoubleArray, KDTree}
import edu.ucr.dblab.pflock.sedona.KDB
import edu.ucr.dblab.pflock.sedona.quadtree._
import edu.ucr.dblab.pflock.Utils._

object MF_Utils {
  case class KDPoint(p: STPoint) extends DoubleArray(p.point)

  case class Pair(p1: STPoint, p2: STPoint){
    def line(implicit G: GeometryFactory): LineString =
      G.createLineString(Array(p1.getCoord, p2.getCoord))
    def wkt(implicit G: GeometryFactory): String =
      s"${line.toText()}\t${p1.oid}\t${p2.oid}\t${line.getLength}"
  }

  case class PairsByKey(cellId: Int, key: Long, pairs: List[Pair], Ps: List[STPoint])

  case class SimplePartitioner(partitions: Int) extends Partitioner {
    override def numPartitions: Int = partitions
    override def getPartition(key: Any): Int = key.asInstanceOf[Int]
  }

  case class MapPartitioner(map: Map[Int, Int]) extends Partitioner {
    override def numPartitions: Int = map.size
    override def getPartition(key: Any): Int = map(key.asInstanceOf[Int])
  }

  def runBFEParallel(points: List[STPoint], cell: Cell)
    (implicit S: Settings, G: GeometryFactory, L: Logger): (Iterator[Disk], Stats) = {

    if(points.isEmpty){
      (Iterator.empty, Stats())
    } else {
      runBFEinParallel(points, cell)
    } 
  }

  def getMaximalsAtCell(pairsByKey: List[PairsByKey], cell: Cell, inner_cell: Cell, stats: Stats,
    debug: Int = -1)
      (implicit S: Settings, G: GeometryFactory, L: Logger): (Iterator[Disk], Stats) = {

    val cellId = TaskContext.getPartitionId
    var Maximals: RTree[Disk] = RTree()

    pairsByKey.map{ tuple =>
      val key   = tuple.key
      val pairs = tuple.pairs
      val Ps    = tuple.Ps

      /////////////////////////////////////////
      if(debug == cellId) println(s"DEBUG|$cellId|\n${Ps.map{_.toString}.mkString("\n")}")
      if(debug == cellId) println(s"DEBUG|$cellId|\n${pairs.map{_.wkt}.mkString("\n")}")

      var tCenters    = 0.0
      var tCandidates = 0.0
      var tMaximals   = 0.0
      pairs.foreach{ pair =>
        val pr = pair.p1
        val ps = pair.p2

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
          val d = disks.filter(_.count >= S.mu)

          ///////////////////////////////////////////////////////////
          if(debug == cellId) L.info(s"${cellId}|Candidates|${d.map(_.toString).mkString("\n")}")

          d
        }
        stats.nCandidates += candidates.size
        tCandidates += tD

        val (_, tM) = timer{
          // cheking if a candidate is not a subset and adding to maximals...
          candidates.foreach{ candidate =>
            Maximals = insertMaximalParallel(Maximals, candidate, acid = 2)
            // use insertMaximalParallelStats to debug and collect statistics...
          }
        }
        tMaximals += tM

      }
      stats.tCenters += tCenters
      stats.tCandidates += tCandidates
      stats.tMaximals += tMaximals
    }

    ////////////////////////
    if(cellId == debug) println(s"${Maximals.entries.map{_.toString}.mkString("\n")}")

    val M = if(cell.isDense){
      Maximals.entries.toList.map(_.value)
        .filter{ maximal => cell.contains(maximal) }
        .filter{ maximal => inner_cell.contains(maximal)}
    } else {
      Maximals.entries.toList.map(_.value)
        .filter{ maximal => cell.contains(maximal) }
    }
    stats.nMaximals = M.size

    (M.toIterator, stats)
  }

  def getPairsAtCell(points_prime: List[STPoint], cell: Cell, stats: Stats)
    (implicit S: Settings, G: GeometryFactory, L: Logger):
      (Iterator[ (List[PairsByKey], Stats) ]) = {


    if(points_prime.isEmpty){
      Iterator.empty
    } else {
      val cellId = TaskContext.getPartitionId

      val (points, tCounts) = timer{
        computeCounts(points_prime)
      }
      stats.nPoints = points.size
      stats.tCounts = tCounts

      val (grid, tGrid) = timer{
        val boundary = new Envelope(cell.mbr)
        boundary.expandBy(S.expansion)
        val grid = Grid(points, envelope = boundary)
        grid.expansion = true
        grid.buildGrid
        //save(s"/tmp/edgesG1_${cellId}.wkt"){ grid.wkt() }
        grid
      }
      stats.tGrid = tGrid

      // for each non-empty cell...
      val pairsByKey = grid.index.keys.map{ key =>
        val ( (_Pr, _Ps), tGrid ) = timer{
          getPrPs(grid, key)
        }
        stats.tGrid += tGrid

        val (pairs, tPairs) = timer{
          getPairs(_Pr, _Ps)
        }
        stats.nPairs += pairs.size
        stats.tPairs += tPairs

        PairsByKey(cellId, key, pairs, _Ps)
      }.toList

      Iterator( (pairsByKey, stats) )
    }
  }

  private def getPrPs(grid: Grid, key: Long): (List[STPoint], List[STPoint]) = {
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

  private def getPairs(Pr: List[STPoint], Ps: List[STPoint])(implicit S: Settings): List[Pair] = {
    if(Ps.size < S.mu) {      // if grid's cells around have not enough points...
      List.empty[Pair]        // return empty...
    } else {
      val H = for{            // get neighborhood of Pr points in Ps...
        r <- Pr
        s <- Ps
        if{ r.distance(s) <= S.epsilon && r.oid < s.oid } // check distance and prune duplicates...
      } yield {
        Pair(r, s)            // store a valid pair...
      }
      if(H.size < S.mu){           // if neighborhood has not enough points...
        List.empty[Pair]      // return empty...
      } else {
        H                     // return List of Pairs...
      }
    }
  }

  def runBFEinParallel(points_prime: List[STPoint], cell: Cell)
    (implicit S: Settings, G: GeometryFactory, L: Logger): (Iterator[Disk], Stats) = {

    val cid = TaskContext.getPartitionId
    val stats = Stats()
    var Maximals: RTree[Disk] = RTree()

    val (points, tCounts) = timer{
      computeCounts(points_prime)
    }
    stats.nPoints = points.size
    stats.tCounts = tCounts

    val (grid, tGrid) = timer{
      val grid = Grid(points)
      grid.buildGrid
      grid
    }
    stats.tGrid = tGrid

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
                    Maximals = insertMaximalParallel(Maximals, candidate)
                    // use insertMaximalParallelStats to debug and collect statistics...
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

    (M.toIterator, stats)
  }

  def insertMaximalParallel(maximals: archery.RTree[Disk], candidate: Disk, acid: Int = -1)
    (implicit S: Settings, L: Logger): archery.RTree[Disk] = {

    val cid = TaskContext.getPartitionId
    if(maximals.entries.size == 0){
      // candidate is the first entry so we insert it...
      val center = archery.Point(candidate.X, candidate.Y)
      val toInsert = Entry(center, candidate)
      maximals.insert(toInsert)
    } else {
      // we query the tree to retrieve maximals around the current candidate...
      val maximals_prime = maximals.search(candidate.bbox(S.epsilon.toFloat))
        .map(_.value)
        .filter{ maximal => maximal.distance(candidate) <= S.epsilon }
      // we check if candidate and current maximal have the same pids...
      maximals_prime.find(_.equals(candidate)) match {
        
        // If so...
        case Some(maximal) => { // maximal pids == current pids...
                                // to be deterministic, we only replace if
                                // new candidate is most left-down disk...
          val Mx = if(candidate < maximal){ // candidate spatial order (left-down most)
                                            // is implemented in Disk class...
            maximals
              .remove(Entry(archery.Point(maximal.X, maximal.Y), maximal))
              .insert(Entry(archery.Point(candidate.X, candidate.Y), candidate))
          } else {
            // the current maximal is still the most left-down disk...
            // so we keep the tree without changes...
            maximals
          }
          Mx
        }
        // None means they are not equal, we then evaluate subsets...
        case None => {
          // we check if candidate is subset of any current maximal...
          if( maximals_prime.exists{ maximal => candidate.isSubsetOf(maximal) } ){
            // candidate is a subset, so we keep the tree without changes...
            maximals
          } else {
            // now we check if maximal(s) is/are subset of candidate...
            val subset_maximals = maximals_prime.filter{ maximal => maximal.isSubsetOf(candidate) }
            subset_maximals match {
              // there is/are maximal(s) subset of candidate...
              case _ if !subset_maximals.isEmpty => {
                // remove subset(s) and insert new candidate...
                val toRemove = subset_maximals.map{ maximal =>
                  val center = archery.Point(maximal.X, maximal.Y)
                  Entry(center, maximal)
                }
                val center = archery.Point(candidate.X, candidate.Y)
                val toInsert = Entry(center, candidate)
                val Mx = maximals.removeAll(toRemove).insert(toInsert)

                Mx
              }
              // candidate is neither subset or superset or equal...
              case _ => {
                // so we insert it...
                val center = archery.Point(candidate.X, candidate.Y)
                val toInsert = Entry(center, candidate)
                val Mx = maximals.insert(toInsert)

                Mx
              }
            } // match
          } // else
        } // case
      } // match
    } // else
  }

  /*** For debugging ***/
  def insertMaximalParallelStats(maximals: archery.RTree[Disk], candidate: Disk, cell: Cell,
    counter: Int) (implicit S: Settings, L: Logger): archery.RTree[Disk] = {

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
          if( maximal.distance(candidate) > S.epsilon ) { // M disjoint C
            // refine stage after filter the tree...
            // maximal and candidate disjoint (pids cannot intersect), we can continue...
          } else if( maximal.equals(candidate) ) { // M equal C
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
          } else if( maximal.isSubsetOf(candidate) ) { // M subset C
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
          } else if( candidate.isSubsetOf(maximal) ) { // C subset M
              flag = 3 
              Mx = maximals
          } else {
            // We check the three alternatives and maximal and candidate are different,
            // we can continue...
          }
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

  def recreateInnerGrid(cell: Cell, expansion: Boolean = false)(implicit S: Settings): List[Cell] = {
    val mbr = cell.mbr

    val epsilon = if(expansion) S.expansion else S.epsilon_prime
    val n = math.ceil(mbr.getWidth  / epsilon)
    val X = (mbr.getMinX until mbr.getMaxX by epsilon).toList :+ mbr.getMaxX
    val m = math.ceil(mbr.getHeight / epsilon)
    val Y = (mbr.getMinY until mbr.getMaxY by epsilon).toList :+ mbr.getMaxY

    (for{
      i <- 0 until n.toInt
      j <- 0 until m.toInt
    } yield {
      val mbr = new Envelope( X(i), X(i + 1), Y(j), Y(j + 1) )
      val gridId = encode(i, j).toInt
      val lin = s"${cell.cid}_$gridId"
      Cell(mbr, gridId, lin)
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
        .textFile(S.dataset).rdd
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

  def loadData[T](implicit spark: SparkSession, S: Settings, G: GeometryFactory, L: Logger)
      : (RDD[Point], STRtree, Map[Int, Cell], Double) = {

    val ( (pointsRaw, nRead), tRead) = timer{
      val pointsRaw = spark.read
        .option("delimiter", "\t")
        .option("header", false)
        .textFile(S.dataset).rdd
        .map { line =>
          val arr = line.split("\t")
          val i = arr(0).toInt
          val x = arr(1).toDouble
          val y = arr(2).toDouble
          val t = arr(3).toInt
          val point = G.createPoint(new Coordinate(x, y))
          point.setUserData(Data(i, t))
          point
        }.cache
      val nRead = pointsRaw.count
      (pointsRaw, nRead)
    }
    log(s"Read|$nRead")
    logt(s"Read|$tRead")

    val (cells, tIndex) = if(false){
      timer{
        // create sedona tree...
        val kdtree = getKDTreeFromPoints(pointsRaw)

        // return map with cells...
        kdtree.getLeafZones().asScala.map{ case(id, envelope) =>
          val cid = id.toInt
          (cid, Cell(envelope, cid, ""))
        }.toMap
      }
    } else {
      timer{
        // create sedona tree...
        val quadtree = Quadtree.getQuadtreeFromPoints(pointsRaw)

        // return map with cells...
        quadtree.getLeafZones.asScala.map{ leaf: QuadRectangle =>
          val cid = leaf.partitionId.toInt
          val lin = leaf.lineage
          val env = leaf.getEnvelope

          (cid, Cell(env, cid, lin))
        }.toMap
      }
    }

    // feed the cells into a JTS RTree for better performance...
    val tree = new STRtree(2)
    cells.values.foreach{ cell =>
      tree.insert(cell.mbr, cell)
    }

    (pointsRaw, tree, cells, tIndex)
  }

  def getKDTreeFromPoints(points: RDD[Point])(implicit S: Settings): KDB = {
    val sample = points.sample(false, S.fraction, 42).collect().toList
    val minX = sample.map(_.getX).min
    val minY = sample.map(_.getY).min
    val maxX = sample.map(_.getX).max
    val maxY = sample.map(_.getY).max
    val envelope = new Envelope(minX, maxX, minY, maxY)
    envelope.expandBy(S.epsilon) // add a pad around the study area for possible disks ...

    val kdtree = new KDB(S.capacity, 16, envelope)
    sample.foreach{ point =>
      val envelope = point.getEnvelopeInternal
      kdtree.insert(envelope)
    }
    kdtree.assignLeafIds()

    kdtree
  }

  def countPairs(pairs: RDD[Stats], cells: Map[Int, Cell]): Int = {
    val pairsPerCell = pairs.mapPartitionsWithIndex{ case(cid, it) =>
      it.map{ p => (cid, p.nPairs) }
    }.collect.toList

    var n = 0
    for{
      cell  <- cells.values
      count <- pairsPerCell if(count._1 == cell.cid)
    } yield {
      cell.nPairs = count._2
      n += count._2
    }
    n
  }

  def getHDFSPath(implicit S: Settings): String = S.dataset.split("/")
    .reverse.tail.reverse.mkString("/")

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

  def diff(testing: List[Disk], validation: List[Disk], points: List[STPoint])
    (implicit S: Settings, G: GeometryFactory, L: Logger): Unit = {
    val test     = testing.map(_.pidsText + "\n").sorted
    val validate = validation.map(_.pidsText + "\n").sorted

    save("/tmp/m1.txt"){test}
    save("/tmp/m2.txt"){validate}
    val diffs = s"diff /tmp/m1.txt /tmp/m2.txt".lineStream_!
    val (diff1_prime, diff2_prime) = diffs.filter(l => l.startsWith("<") || l.startsWith(">"))
      .partition(_.startsWith("<"))

    val fails   = diff1_prime.map(_.substring(2)).toList
    val missing = diff2_prime.map(_.substring(2)).toList
    
    (fails, missing) match {
      case _ if  fails.isEmpty &&  missing.isEmpty => log("Pass!!")
      case _ if  fails.isEmpty && !missing.isEmpty => {
        missing.sorted.foreach{println}
        log(s"${missing.size} missing disks not reported by testing set...")
      }
      case _ if !fails.isEmpty &&  missing.isEmpty => {
        fails.sorted.foreach{println}
        log(s"${fails.size} extra disks reported by testing set...")
      }
      case _ if !fails.isEmpty && !missing.isEmpty => {
        fails.sorted.foreach{println}
        log(s"${fails.size} extra disks reported by testing set...")
        println
        missing.sorted.foreach{println}
        log(s"${missing.size} missing disks not reported by testing set...")
      }
      case _ => log("Unknown exception in validation...")
    }
  }

  def validate(testing: List[Disk], validation: List[Disk], points: List[STPoint])
    (implicit S: Settings, G: GeometryFactory, L: Logger): Unit = {

    @annotation.tailrec
    def compare_recursive(testing: List[Disk], validation: KDTree, fails: List[Disk], found: List[Disk])
        : (List[Disk], List[Disk]) = {
      testing match {
        case head :: tail => {
          val entry = new DoubleArray(head.center, head.pidsText)
          val hood = validation.pointsWithinRadiusOf(entry, S.r).asScala.map{ entry =>
            val center = entry.point
            val pids  = entry.pids.split(" ").map(_.toInt).sorted.toList
            Disk(center, pids)
          }

          hood.find(_.pidsText.compare(head.pidsText) == 0) match { // equals compare pids in each disk...
            case Some(v) => { // disk found!
              compare_recursive(tail, validation, fails, found :+ v)
            }
            case None => { // disk not found...
              compare_recursive(tail, validation, fails :+ head, found)
            }
          }
        }
        case Nil => {
          (fails, found)
        }
      }
    }

    def compare(testing: List[Disk], validation: List[Disk]): (List[Disk], List[Disk]) = {
      val kdtree = new KDTree()
      val entries = validation.map{ disk =>
        new DoubleArray(disk.center, disk.pidsText)
      }.asJava
      kdtree.buildtree(entries)

      val (fails, found) = compare_recursive(testing, kdtree, List.empty, List.empty)
      val missing = validation.filterNot{ valid => found.exists(_.equals(valid)) }

      (fails, missing)
    }

    val (fails, missing) = compare(testing, validation)

    (fails, missing) match {
      case _ if  fails.isEmpty &&  missing.isEmpty => log("Pass!!")
      case _ if  fails.isEmpty && !missing.isEmpty => {
        missing.sortBy(_.pidsText).foreach{println}
        log(s"${missing.size} missing disks not reported by testing set...")
      }
      case _ if !fails.isEmpty &&  missing.isEmpty => {
        fails.sortBy(_.pidsText).foreach{println}
        log(s"${fails.size} extra disks reported by testing set...")
      }
      case _ if !fails.isEmpty && !missing.isEmpty => {
        fails.sortBy(_.pidsText).foreach{println}
        log(s"${fails.size} extra disks reported by testing set...")
        println
        missing.sortBy(_.pidsText).foreach{println}
        log(s"${missing.size} missing disks not reported by testing set...")
      }
      case _ => log("Unknown exception in validation...")
    }
  }

  def spatialValidation(disk: Disk, points: KDTree)(implicit S: Settings, G: GeometryFactory): Boolean = {
    val query = new DoubleArray(disk.center, disk.pidsText)
    points.pointsWithinRadiusOf(query, S.epsilon + S.tolerance).asScala.map{ r =>
      val center = r.point

      ???
    }

    ???
  }
}

