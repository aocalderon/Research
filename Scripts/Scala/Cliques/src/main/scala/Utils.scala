package edu.ucr.dblab.pflock

import com.vividsolutions.jts.algorithm.MinimumBoundingCircle
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel, Geometry}
import com.vividsolutions.jts.geom.{Envelope, Coordinate, Point, Polygon}
import com.vividsolutions.jts.index.strtree.STRtree
import com.vividsolutions.jts.io.WKTReader
import org.geotools.geometry.jts.JTS

import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext

import org.apache.commons.math3.geometry.euclidean.twod.DiskGenerator
import org.apache.commons.math3.geometry.euclidean.twod.Vector2D
import org.apache.commons.math3.geometry.enclosing.{SupportBallGenerator, EnclosingBall}

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.Random
import sys.process._

import org.slf4j.{Logger, LoggerFactory}

import edu.ucr.dblab.pflock.spmf.{Transactions, Transaction, AlgoLCM2}

import archery._

import edu.ucr.dblab.pflock.pbk.PBK.bk
import edu.ucr.dblab.pflock.welzl.Welzl

object Utils {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  /*** Case Class ***/
  case class Settings(
    input: String = "",
    epsilon_prime: Double = 10.0,
    mu: Int = 3,
    delta: Int = 5,
    capacity: Int = 200,
    fraction: Double = 0.1,
    tolerance: Double = 1e-3,
    appId: String = "",
    tag: String = "",
    method: String = "BFE",
    debug: Boolean = false
  ){
    val scale: Double = 1 / tolerance
    val epsilon: Double = epsilon_prime + tolerance
    val r: Double = (epsilon_prime / 2.0) + tolerance
    val r2: Double = math.pow(epsilon_prime / 2.0, 2) + tolerance
    var partitions: Int = 1

    def info: String = s"$appId|$partitions|$epsilon_prime|$mu|$delta|$method"
  }

  case class STPoint(point: Point, cid: Int = 0){
    val userData = point.getUserData.asInstanceOf[Data]
    val oid = userData.id
    val tid = userData.t
    var count = 0

    def envelope: Envelope = point.getEnvelopeInternal

    def distance(other: STPoint): Double = point.distance(other.point)

    def distanceToPoint(p: Point): Double = point.distance(p)
    
    def X: Double = point.getX

    def Y: Double = point.getY

    def getNeighborhood(points: List[STPoint])(implicit settings: Settings): List[STPoint] = {
      for{
        point <- points if{ this.distance(point) <= settings.epsilon }
      } yield {
        point
      }
    }

    def getCoord: Coordinate = new Coordinate(X, Y)

    def toText: String = s"${point.toText()}"

    def wkt: String = s"${point.toText()}\t$cid\t$oid\t$tid"

    def expandEnvelope(eps: Double): Envelope = {
      val envelope = point.getEnvelopeInternal
      envelope.expandBy(eps)
      envelope
    }

    override def toString: String = s"${point.getX}\t${point.getY}\t$cid\t$oid\t$tid\t$count"

  }

  case class Data(id: Int, t: Int){
    override def toString = s"$id\t$t"
  }

  case class Cell(mbr: Envelope, cid: Int, lineage: String){
    val wkt: String = toString + "\n"

    val bbox: Box = Box(mbr.getMinX.toFloat, mbr.getMinY.toFloat,
      mbr.getMaxX.toFloat, mbr.getMaxY.toFloat)

    def contains(disk: Disk): Boolean = mbr.contains(disk.X, disk.Y)

    override def toString: String = s"${JTS.toGeometry(mbr).toText}\t$cid\t$lineage"
  }

  case class Disk(center: Point, pids: List[Int],
    support: List[Int] = List.empty[Int]) extends Ordered [Disk]{

    var did: Int = -1
    var subset: Boolean = false
    val X: Float = center.getX.toFloat
    val Y: Float = center.getY.toFloat
    val count: Int = pids.size
    val pidsText = pids.sorted.mkString(" ")
    
    def envelope: Envelope = center.getEnvelopeInternal

    def getExpandEnvelope(r: Double): Envelope = {
      val envelope = center.getEnvelopeInternal
      envelope.expandBy(r)
      envelope
    }

    def pidsSet: Set[Int] = pids.toSet

    def intersect(other: Disk): Set[Int] = this.pidsSet.intersect(other.pidsSet)

    def bbox(r: Float): Box = Box(X - r, Y - r, X + r, Y + r)

    def containedBy(cell: Cell): Boolean = cell.contains(this)

    def distance(other: Disk): Double = {
      center.distance(other.center)
    }

    def isSubsetOf(other: Disk): Boolean = pidsSet.subsetOf(other.pidsSet)

    override def toString: String = s"${did}\t${pids.sorted.mkString(" ")}\t${X}\t${Y}"

    def wkt: String = s"${center.toText}\t${pidsText}"

    def equals(other: Disk): Boolean = this.pidsText == other.pidsText

    def compare(other: Disk): Int = 
      this.X.compare(other.X) match {
        case -1 => -1
        case  0 =>  this.Y compare other.Y 
        case  1 =>  1
      }

    def duplicates(tree: RTree[Disk])(implicit settings: Settings): Seq[Disk] = tree
      .search(this.bbox(settings.epsilon.toFloat))
      .map(_.value)
      .filter(_.equals(this))
  }

  case class Grid(points: List[STPoint]){
    var minx: Double = 0.0
    var miny: Double = 0.0
    var n: Int = 0
    var m: Int = 0
    var index: Map[Long, List[STPoint]] = Map.empty

    def buildGrid(implicit settings: Settings): Unit = {
      minx = points.minBy(_.X).X
      miny = points.minBy(_.Y).Y
      val grid = points.filter(_.count >= settings.mu).map{ point =>
        val i = math.floor( (point.X - minx) / settings.epsilon_prime ).toInt
        val j = math.floor( (point.Y - miny) / settings.epsilon_prime ).toInt
        (encode(i, j), point)
      }.groupBy(_._1)

      index = grid.mapValues(_.map(_._2))
    }

    def pointsToText: List[String] = {
      index.values.flatten.map{_.wkt + "\n"}.toList
    }

    def toText: List[String] = {
      index.map{ case(key, points) =>
        val (i, j) = decode(key)
        points.map{ point =>
          val wkt = point.toText
          val oid = point.oid

          s"$wkt\t$key\t($i $j)\t$oid\n"
        }
      }.flatten.toList
    }

    def wkt(limit: Int = 2000)
      (implicit settings: Settings, geofactory: GeometryFactory): Seq[String] = {
      if(!index.isEmpty){
        val epsilon = settings.epsilon
        val maxx = index.values.flatten.maxBy(_.X).X
        val maxy = index.values.flatten.maxBy(_.Y).Y
        n = math.ceil( (maxx - minx) / epsilon ).toInt
        m = math.ceil( (maxy - miny) / epsilon ).toInt

        if(n * m < limit){
          for{
            i <- 0 until n
            j <- 0 until m
          } yield {
            val x1 = minx + (i * epsilon)
            val x2 = x1 + epsilon
            val y1 = miny + (j * epsilon)
            val y2 = y1 + epsilon
            val envelope = new Envelope(x1,x2,y1,y2)
            val polygon = JTS.toGeometry(envelope)
            val wkt = polygon.toText
            val k = encode(i, j)

            s"$wkt\t($i $j)\t$k\n"
          }
        } else {
          Seq.empty
        }
      } else {
        Seq.empty
      }
    }
  }

  case class MBC(center: Point, radius: Double, points: List[Point])

  case class Stats(var nPoints: Int = 0, var nPairs: Int = 0, var nCenters: Int = 0,
    var nCandidates: Int = 0, var nMaximals: Int = 0,
    var nCliques: Int = 0, var nMBC: Int = 0,
    var tCounts: Double = 0.0, var tRead: Double = 0.0, var tGrid: Double = 0.0, 
    var tCliques: Double = 0.0, var tMBC: Double = 0.0,
    var tPairs: Double = 0.0, var tCenters: Double = 0.0,
    var tCandidates: Double = 0.0, var tMaximals: Double = 0.0){

    def print(printTotal: Boolean = true)(implicit logger: Logger, S: Settings): Unit = {
      log(s"Points     |${nPoints}")
      if(S.method.contains("CMBC")){
        logt(s"Cliques   |${nCliques}")
        logt(s"MBCs      |${nMBC}")
      }
      log(s"Pairs      |${nPairs}")
      log(s"Centers    |${nCenters}")
      log(s"Candidates |${nCandidates}")
      log(s"Maximals   |${nMaximals}")
      logt(s"Count     |${tCounts}")
      logt(s"Grid      |${tGrid}")
      logt(s"Read      |${tRead}")
      if(S.method.contains("CMBC")){
        logt(s"Cliques   |${tCliques}")
        logt(s"MBCs      |${tMBC}")
      }
      logt(s"Pairs     |${tPairs}")
      logt(s"Centers   |${tCenters}")
      logt(s"Candidates|${tCandidates}")
      logt(s"Maximals  |${tMaximals}")
      if(printTotal){
        val tTotal = tMaximals + tCandidates + tCenters + tPairs + tCliques + tRead + tGrid + tCounts
        logt(s"Total     |${tTotal}")
      }
    }
  }

  case class DataFiles(
    points:   List[Point],
    pairs:    List[(Point, Point)],
    centers:  List[Point],
    disks:    List[Disk],
    maximals: List[Disk]
  )

  /*** CMBC Functions ***/

  def getMBCsPerClique(points: List[STPoint])
    (implicit S: Settings, G: GeometryFactory): (List[MBC], Int, Double, Double) = {

    val vertices = points.map{_.point}
    val edges = getEdges(points)
    
    // finding cliques...
    val (cliques, tCli) = timer{ bk(vertices, edges).iterator.filter(_.size >= S.mu).toList }
    val nCli = cliques.size

    // finding MBC in each clique...
    val (mbcs, tMBC) = timer{
      cliques.map{ points_per_clique =>
        val mbc = Welzl.mbc(points_per_clique)
        val radius = round(mbc.getRadius)
        val center = G.createPoint(new Coordinate(mbc.getCenter.getX,
          mbc.getCenter.getY))
        MBC(center, radius, points_per_clique)
      }
    }

    (mbcs, nCli, tCli, tMBC)
  }

  def partitionByRadius(mbcs: List[MBC])
    (implicit settings: Settings): (List[Disk], List[STPoint]) ={

    // dividing MBCs by radius less than epsilon...
    val (maximals_prime, disks_prime) = mbcs.partition{ _.radius < settings.r }
    // returning MBCs less than epsilon as maximals disks (maximals1)...
    val maximals1 = maximals_prime.map{ mbc =>
      val pids = mbc.points.map(_.getUserData.asInstanceOf[Data].id)
      
      Disk(mbc.center, pids, List.empty)
    }.toList

    // returning remaining points in MBCs greater than epsilon as list of points (points_prime)...
    val points_prime = pointsToSTPoint(disks_prime.flatMap{_.points}.toList)

    (maximals1, points_prime)
  }
  

  /*** BFE Functions ***/

  def buildGrid(points: List[STPoint])(implicit settings: Settings): Map[Long, List[STPoint]] = {
    val minx = points.minBy(_.X).X
    val miny = points.minBy(_.Y).Y
    val grid = points.filter(_.count >= settings.mu).map{ point =>
      val i = math.floor( (point.X - minx) / settings.epsilon_prime ).toInt
      val j = math.floor( (point.Y - miny) / settings.epsilon_prime ).toInt
      (encode(i, j), point)
    }.groupBy(_._1)

    grid.mapValues(_.map(_._2))
  }

  def findPairs(points: List[STPoint])(implicit geofactory: GeometryFactory, settings: Settings):
      List[(STPoint, STPoint)] = {
    for {
      a <- points
      b <- points
      if{ a.oid < b.oid & a.distance(b) <= settings.epsilon }
    } yield {
      (a, b)
    }
  }

  def findCenters(points: List[STPoint], epsilon: Double, r2: Double)
      (implicit geofactory: GeometryFactory, settings: Settings): List[Point] = {
    val centers = for {
      a <- points
      b <- points if {
        val id1 = a.oid
        val id2 = b.oid

        id1 < id2 & a.distance(b) <= epsilon
      }
    } yield {
      calculateCenterCoordinates(a.point, b.point)
    }

    centers.flatten    
  }

  def pruneCandidates(disks_prime: List[Disk])(implicit settings: Settings): List[Disk] = {
    val disks = disks_prime.zipWithIndex.map{ case(disk, did) =>
      disk.did = did
      disk
    }
    for{
      c1 <- disks
      c2 <- disks
      if(c1.distance(c2) <= settings.epsilon & c1.did < c2.did)
    } yield {
      if(c1.isSubsetOf(c2)){
        c1.subset = true
      }else if(c2.isSubsetOf(c1)){
        c2.subset = true
      }
      List(c1, c2)
    }

    disks.filter(_.subset != true)
  }

  def computePairs(points: List[Point], epsilon: Double): List[(Point, Point)] = {
    for {
      a <- points
      b <- points if {
        val id1 = a.getUserData.asInstanceOf[String].split("\t")(0).toInt
        val id2 = b.getUserData.asInstanceOf[String].split("\t")(0).toInt
          (id1 < id2) && (a.distance(b) <= epsilon)
      }
    } yield {
      (a, b)
    }
  }

  def getEdges(points_prime: List[STPoint])(implicit S: Settings): List[(Point, Point)] = {
    val tree = new STRtree(200)
    points_prime.foreach{ point => tree.insert(point.envelope, point) }

    for {
      a <- points_prime
      b <- tree.query(a.expandEnvelope(S.epsilon)).asScala.map(_.asInstanceOf[STPoint])
      if {
          (a.oid < b.oid) && (a.distance(b) <= S.epsilon)
      }
    } yield {
      (a.point, b.point)
    }
  }

  def insertMaximalLCM(maximals: List[Disk], candidate: Disk): List[Disk] = {
    if(maximals.isEmpty){
      maximals :+ candidate
    } else {
      if( maximals.exists( maximal => candidate.isSubsetOf(maximal) ) ){
        maximals
      } else {
        if( maximals.exists( maximal => maximal.isSubsetOf(candidate) ) ){
          maximals.filterNot( maximal => maximal.isSubsetOf(candidate) ) :+ candidate
        } else {
          maximals :+ candidate
        }
      }
    }
  }

  def insertMaximal(maximals: List[Disk], candidate: Disk): List[Disk] = {
    if(maximals.isEmpty){
      maximals :+ candidate
    } else {
      if( maximals.exists( maximal => candidate.isSubsetOf(maximal) ) ){
        maximals
      } else {
        if( maximals.exists( maximal => maximal.isSubsetOf(candidate) ) ){
          maximals.filterNot( maximal => maximal.isSubsetOf(candidate) ) :+ candidate
        } else {
          maximals :+ candidate
        }
      }
    }
  }

  def insertMaximals(maximals: archery.RTree[Disk], candidates: Iterable[Disk])
    (implicit settings: Settings): archery.RTree[Disk] = {

    if(candidates.isEmpty){
      maximals
    } else {
      if(maximals.entries.size == 0){
        val toInsert = candidates.map{ candidate =>
          val center = archery.Point(candidate.X, candidate.Y)
          Entry(center, candidate)
        }
        maximals.insertAll(toInsert)
      } else {
        val empty = Iterable.empty[Entry[Disk]]
        val R = candidates.map{ candidate =>
          val maximals_prime = maximals.search(candidate.bbox(settings.epsilon.toFloat)).map(_.value)

          if( maximals_prime.exists( maximal => candidate.isSubsetOf(maximal) ) ){
            (empty, empty)
          } else {
            if( maximals_prime.exists( maximal => maximal.isSubsetOf(candidate) ) ){
              val toRemove = maximals_prime.filter( maximal => maximal.isSubsetOf(candidate) )
                .map{ maximal =>
                  val center = archery.Point(maximal.X, maximal.Y)
                  Entry(center, maximal)
                }
              val center = archery.Point(candidate.X, candidate.Y)
              val toInsert = Iterable(Entry(center, candidate))
              (toInsert, toRemove)
            } else {
              val center = archery.Point(candidate.X, candidate.Y)
              val toInsert = Iterable(Entry(center, candidate))
              (toInsert, empty)
            }
          }
          
        }
        val toInsert = R.map{_._1}.flatten
        val toRemove = R.map{_._2}.flatten.toList.distinct
        maximals.insertAll(toInsert).removeAll(toRemove)
      }
    }
  }

  def insertMaximal(maximals: archery.RTree[Disk], candidate: Disk)
    (implicit settings: Settings): archery.RTree[Disk] = {

    if(maximals.entries.size == 0){
      val center = archery.Point(candidate.X, candidate.Y)
      val toInsert = Entry(center, candidate)
      maximals.insert(toInsert)
    } else {
      val maximals_prime = maximals.search(candidate.bbox(settings.epsilon.toFloat)).map(_.value)

      if( maximals_prime.exists( maximal => candidate.isSubsetOf(maximal) ) ){
        maximals
      } else {
        if( maximals_prime.exists( maximal => maximal.isSubsetOf(candidate) ) ){
          val toRemove = maximals_prime.filter( maximal => maximal.isSubsetOf(candidate) )
            .map{ maximal =>
              val center = archery.Point(maximal.X, maximal.Y)
              Entry(center, maximal)
            }
          val center = archery.Point(candidate.X, candidate.Y)
          val toInsert = Entry(center, candidate)
          maximals.removeAll(toRemove).insert(toInsert)
        } else {
          val center = archery.Point(candidate.X, candidate.Y)
          val toInsert = Entry(center, candidate)
          maximals.insert(toInsert)
        }
      }
    }
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

  def pruneDisks(disks: List[Disk])
    (implicit geofactory: GeometryFactory): List[Disk] = {

    val transactions = disks.map{ disk =>
      ((disk.center.getX, disk.center.getY), disk.pids.sorted.mkString(" "))
    }.groupBy(_._2).map{ disk =>
      val pids = disk._1
      val (x, y) = disk._2.head._1

      new Transaction(x, y, pids)
    }.toList

    val data = new Transactions(transactions.asJava, 0)
    val lcm = new AlgoLCM2()
    lcm.run(data)

    lcm.getPointsAndPids.asScala
      //.filter(_.getItems.size >= mu)  // CHECK: is it safe to filter by mu?
      .map{ m =>
        val pids = m.getItems.toList.map(_.toInt).sorted
        val x = m.getX
        val y = m.getY
        val center = geofactory.createPoint(new Coordinate(x, y))
        Disk(center, pids) // FIXME: update support list...
      }.toList
  }

  def pruneDisks2(disks: List[Disk])
    (implicit geofactory: GeometryFactory): List[Disk] = {

    val transactions = disks.map{ disk =>
      (disk.pids.sorted.mkString(" "), disk)
    }.groupBy(_._1).map{ case(pids, disks) =>
        val disk = disks.head._2
        val center = disk.center

        new Transaction(center, pids)
    }.toList

    val data = new Transactions(transactions.asJava, 0)
    val lcm = new AlgoLCM2()
    lcm.run(data)

    lcm.getPointsAndPids.asScala
      .map{ m =>
        val pids = m.getItems.toList.map(_.toInt).sorted
        val center = m.getCenter
        Disk(center, pids, center.getUserData.asInstanceOf[List[Int]])
      }.toList
  }

  def calculateCenterCoordinates(p1: Point, p2: Point)
    (implicit geofactory: GeometryFactory, settings: Settings): List[Point] = {

    val X: Double = p1.getX - p2.getX
    val Y: Double = p1.getY - p2.getY
    val D2: Double = math.pow(X, 2) + math.pow(Y, 2)
    if (D2 != 0.0){
      val root: Double = math.sqrt(math.abs(4.0 * (settings.r2 / D2) - 1.0))
      val h1: Double = ((X + Y * root) / 2) + p2.getX
      val k1: Double = ((Y - X * root) / 2) + p2.getY
      val h2: Double = ((X - Y * root) / 2) + p2.getX
      val k2: Double = ((Y + X * root) / 2) + p2.getY
      val h = geofactory.createPoint(new Coordinate(h1,k1))
      val k = geofactory.createPoint(new Coordinate(h2,k2))

      List(h, k)
    } else {
      val p2_prime = geofactory.createPoint(new Coordinate(p2.getX + settings.tolerance,
        p2.getY))
      calculateCenterCoordinates(p1, p2_prime)
    }
  }

  def getPointsAroundCenter(center: Point, points: List[STPoint])
      (implicit settings:Settings): Disk = {
    val pids = for{
      point <- points if { point.distanceToPoint(center) <= settings.r }
    } yield {
      point.oid
    }

    Disk(center, pids)
  }

  /*** Misc Functions ***/
  def encode(x: Int, y: Int): Long = {
    return if(x >= y){ x * x + x + y } else { y * y + x }
  }

  def decode(z: Long): (Int, Int) = {
    val b = math.floor(math.sqrt(z.toDouble))
    val a = z - b * b

    return if(a < b){ (a.toInt, b.toInt) } else { (b.toInt, (a - b).toInt) }
  }

  def computeCounts(points_prime: List[STPoint])(implicit S: Settings): List[STPoint] = {
    val tree = new STRtree(200)
    points_prime.foreach{ point => tree.insert(point.envelope, point) }

    val join = points_prime.flatMap{ p1 =>
      val envelope = p1.envelope
      envelope.expandBy(S.epsilon)
      val hood = tree.query(envelope).asScala.map{_.asInstanceOf[STPoint]}
      for{
        p2 <- hood if{ p1.distance(p2) <= S.epsilon }
      } yield {
        (p1, 1)
      }
    }
    /*
    val join = for{
      p1 <- points_prime
      p2 <- points_prime
      if{ p1.distance(p2) <= S.epsilon }
    } yield {
      (p1, 1)
    }
    */
    val points = join.groupBy(_._1).map{ case(point, counts) =>
      point.count = counts.size
      point
    }.toList
    points
  }

  def readPoints(input: String, isWKT: Boolean = false)
    (implicit geofactory: GeometryFactory, settings: Settings, logger: Logger): List[STPoint] = {

    val buffer = Source.fromFile(input)
    val points = buffer.getLines.zipWithIndex.toList
      .map{ line =>
        if(isWKT){
          val reader = new WKTReader(geofactory)
          val i = line._2.toInt
          val t = 0
          val point = reader.read(line._1).asInstanceOf[Point]

          point.setUserData(Data(i, t))
          STPoint(point)
        } else {
          val arr = line._1.split("\t")
          val i = arr(0).toInt
          val x = arr(1).toDouble
          val y = arr(2).toDouble
          val t = arr(3).toInt
          val point = geofactory.createPoint(new Coordinate(x, y))

          point.setUserData(Data(i, t))
          STPoint(point)
        }
      }
    buffer.close
    points
  }

  def pointsToSTPoint(points_prime: List[Point])(implicit settings: Settings): List[STPoint] = {
    val stpoints = points_prime.map(p => STPoint(p)).distinct
    val points = for{
      p1 <- stpoints
      p2 <- stpoints
      if{ p1.distance(p2) <= settings.epsilon }
    } yield {
      (p1, 1)
    }
    points.groupBy(_._1).map{ case(point, counts) =>
      point.count = counts.size
      point
    }.toList
  }

  def generateData(n: Int, width: Double, height: Double, filename: String): Unit = {
    val points = (0 until n).map{ i =>
      val x = Random.nextDouble * width
      val y = Random.nextDouble * height

      s"$i\t$x\t$y\t0\n"
    }
    save(filename){
      points
    }
  }

  def saveData(implicit data: DataFiles, geofactory: GeometryFactory): Unit = {
    save("/tmp/edgesPoints.wkt"){
      data.points.map{ point =>
        val wkt = point.toText
        val id  = point.getUserData
        s"$wkt\t$id\n"
      }
    }
    save("/tmp/edgesPairs.wkt"){
      data.pairs.map{ case(p1, p2) =>
        val coords = Array(p1.getCoordinate, p2.getCoordinate)
        val line = geofactory.createLineString(coords)
        val wkt = line.toText
        val id  = s"${p1.getUserData}\t${p2.getUserData}"
        s"$wkt\t$id\n"
      }
    }
    save("/tmp/edgesCenters.wkt"){
      data.centers.map{ center =>
        val wkt = center.toText
        val ids = center.getUserData.asInstanceOf[List[Int]]
        val id1 = ids(0)
        val id2 = ids(1)

        s"$wkt\t$id1\t$id2\n"
      }
    }
    save("/tmp/edgesDisks.wkt"){
      data.disks.map{ disk =>
        val wkt     = disk.center.toText
        val pids    = disk.pids.sorted.mkString(" ")
        val support = disk.support.sorted.mkString(" ")

        s"$wkt\t$pids\t$support\n"
      }
    }
    save("/tmp/edgesMaximals.wkt"){
      data.maximals.map{ disk =>
        val wkt     = disk.center.toText
        val pids    = disk.pids.sorted.mkString(" ")
        val support = disk.support.sorted.mkString(" ")

        s"$wkt\t$pids\t$support\n"
      }
    }
  }

  def checkPoints(x: String)(implicit data: DataFiles): Unit = {
    val l = x.split(" ").map(_.toInt).toSet
    val s = data.points.filter(x => l.contains(x.getUserData.asInstanceOf[Int]))
    save("/tmp/edgesSample.wkt"){
      s.map{ p =>
        val wkt = p.toText
        val id  = p.getUserData
        s"$wkt\t$id\n"
      }
    }
  }

  def clocktime: Long = System.nanoTime()

  def log(msg: String)(implicit logger: Logger, settings: Settings): Unit = {
    logger.info(s"INFO|${settings.info}|$msg")
  }

  def logt(msg: String)(implicit logger: Logger, settings: Settings): Unit = {
    logger.info(s"TIME|${settings.info}|$msg")
  }

  def round(x: Double)(implicit settings: Settings): Double = {
    val decimal_positions = math.log10(settings.scale).toInt
    BigDecimal(x).setScale(decimal_positions, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  def timer[R](msg: String)(block: => R): R = {
    val t0 = clocktime
    val result = block    // call-by-name
    val t1 = clocktime
    logger.info("TIME|%-30s|%6.2f".format(msg, (t1 - t0) / 1e9))
    result
  }

  def timer[R](block: => R): (R, Double) = {
    val t0 = clocktime
    val result = block    // call-by-name
    val t1 = clocktime
    val time = (t1 - t0) / 1e9
    (result, time)
  }

  def debug[R](block: => R)(implicit S: Settings): Unit = { if(S.debug) block }

  def save(filename: String)(content: Seq[String]): Unit = {
    val start = clocktime
    val f = new java.io.PrintWriter(filename)
    f.write(content.mkString(""))
    f.close
    val end = clocktime
    val time = "%.2f".format((end - start) / 1e9)
    println(s"Saved ${filename}\tin\t${time}s\t[${content.size} records].")
  }

  def checkMF(maximalsMF: List[Disk])
    (implicit geofactory: GeometryFactory, settings: Settings, logger: Logger): Unit = {

    val points = readPoints(s"/home/acald013/Research/${settings.input}")
    val (maximalsBFE, statsBFE) = BFE.run(points)
    statsBFE.print()
    save("/tmp/edgesBFE.wkt"){ maximalsBFE.map(_.wkt + "\n") }

    checkMaximalDisks(maximalsBFE, maximalsMF, points)
  }

  def checkMaximals(points: List[STPoint], bfe2file: String = "/tmp/edgesMaximals.wkt")
    (implicit geofactory: GeometryFactory, settings: Settings, logger: Logger): Unit = {

    val out = timer(s"${settings.info}|Bfe0"){
      s"bfe ${settings.input} ${settings.epsilon_prime.toInt} ${settings.mu} 1" !!
    }
    parseBFEOutput(out).foreach(log)
    val bfe1file = s"/tmp/BFE_E${settings.epsilon_prime.toInt}_M${settings.mu}_D1.txt"

    checkMaximalsByFile(bfe1file, bfe2file, points)
  }

  private def parseBFEOutput(out: String): List[String] = {
    val o = out.split("\n").filter(_.startsWith("totalPairs")).flatMap{ line =>
      val arr = line.split("\t").map(_.replace(":", ""))
      arr
    }.toList

    o.zip(o.tail).map{case(a, b) => s"$a|$b"}.filter(_.startsWith("total"))
  }


  private def checkMaximalsByFile(bfe1file: String, bfe2file: String, points: List[STPoint])
    (implicit geofactory: GeometryFactory, settings: Settings, logger: Logger): Unit = {

    val buffer1 = Source.fromFile(bfe1file)
    val center = geofactory.createPoint(new Coordinate(0,0))
    val bfe1 = buffer1.getLines.map{ line =>
      val arr = line.split("\t")
      val pids = arr(2).split(" ").map(_.toInt).sorted.toList

      Disk(center, pids)
    }.toList
    buffer1.close

    val buffer2 = Source.fromFile(bfe2file)
    val reader = new WKTReader(geofactory)
    val bfe2 = buffer2.getLines.map{ line =>
      val arr = line.split("\t")
      val center = reader.read(arr(0)).asInstanceOf[Point]
      val pids = arr(1).split(" ").map(_.toInt).sorted.toList

      Disk(center, pids)
    }.toList
    buffer2.close

    checkMaximalDisks(bfe1, bfe2, points)
  }

  private def checkMaximalDisks(bfe1_prime: List[Disk], bfe2_prime: List[Disk], points: List[STPoint])
    (implicit geofactory: GeometryFactory, settings: Settings, logger: Logger): Unit = {

    val bfe1 = bfe1_prime.map(_.pidsText)
    save("/tmp/bf1.txt"){bfe1.sorted.map(_ + "\n")}

    val bfe2 = bfe2_prime.map(_.pidsText)
    save("/tmp/bf2.txt"){bfe2.sorted.map(_ + "\n")}

    val diffs = "diff -s /tmp/bf1.txt /tmp/bf2.txt".lineStream_!
    val (diff1_prime, diff2_prime) = diffs.filter(l => l.startsWith("<") || l.startsWith(">"))
      .partition(_.startsWith("<"))

    println("<")
    val diff1 = diff1_prime.map(_.substring(2)).toList
    diff1.foreach{println}

    println(">")
    val diff2 = diff2_prime.map(_.substring(2)).toList
    diff2.foreach{println}

    if(diff1.isEmpty && diff2.isEmpty){
        log(s"Maximals|OK!!")
    } else {
      val tree = new STRtree(200)
      points.foreach{ point => tree.insert(point.envelope, point) }

      val checks = for{
        pids <- diff2
        maximal <- bfe2_prime if{ maximal.pidsText == pids }
      } yield {
        val envelope = maximal.getExpandEnvelope(settings.r + settings.tolerance)
        val hood = tree.query(envelope).asScala.map{ _.asInstanceOf[STPoint] }
          .filter(_.distanceToPoint(maximal.center) <= settings.r + settings.tolerance)

        val pids1 = maximal.pidsSet
        val pids2 = hood.map(_.oid).toSet

        val valid = pids1.subsetOf(pids2)
        (maximal, valid)
      }

      val valids = checks.map(_._2).reduce(_ & _)
      //println(s"are our maximals valid? $valids")
      if( valids ){
        //println(s"our maximals contains or are contained by theirs?")
        log(checkMaximalsDiff(diff1, diff2))
      } else {
        val mistakes = checks.filterNot(_._2).map{_._1.wkt}.mkString("\n")
        log(s"Maximals|ERR1 Fakes\n${mistakes}")
      }
    }
  }

  private def checkMaximalsDiff(theirs_prime: List[String], ours_prime: List[String])
    (implicit geofactory: GeometryFactory, settings: Settings, logger: Logger): String = {

    val theirs = theirs_prime.map{_.split(" ").map(_.toInt).toSet}
    val ours = ours_prime.map{_.split(" ").map(_.toInt).toSet}

    val missing = theirs.map{ their =>
      val isSubset = ours.exists( our => their.subsetOf(our) )
      (their, isSubset)
    }.filterNot(_._2).map{_._1.toList.sorted.mkString(" ")}

    if(missing.isEmpty){
      "Maximals|OK!"
    } else {
      s"Maximals|ERR2 Missings\n${missing.mkString("\n")}\n"
    }
  }

  def printParams(args: Seq[String])(implicit S: Settings): Unit = {
    val p = args.filterNot(_ == "--debug")
    val pp = p.zip(p.tail).filter{ case(a, b) => a.startsWith("--")}
      .map{ case(a,b) => s"${a.replace("--", "")}|$b"}
    pp.foreach{ param =>
      logger.info(s"PARAMS|${S.appId}|${param}")
    }
  }  
}


import org.rogach.scallop._

class BFEParams(args: Seq[String]) extends ScallopConf(args) {
  val tolerance: ScallopOption[Double]  = opt[Double]  (default = Some(1e-3))
  val input:     ScallopOption[String]  = opt[String]  (default = Some(""))
  val epsilon:   ScallopOption[Double]  = opt[Double]  (default = Some(10.0))
  val mu:        ScallopOption[Int]     = opt[Int]     (default = Some(5))
  val capacity:  ScallopOption[Int]     = opt[Int]     (default = Some(100))
  val fraction:  ScallopOption[Double]  = opt[Double]  (default = Some(0.01))
  val tag:       ScallopOption[String]  = opt[String]  (default = Some(""))
  val output:    ScallopOption[String]  = opt[String]  (default = Some("/tmp"))
  val debug:     ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val method:    ScallopOption[String]  = opt[String]  (default = Some("BFE"))
  val master:    ScallopOption[String]  = opt[String]  (default = Some("local[10]"))

  verify()
}
