package edu.ucr.dblab.pflock

import com.vividsolutions.jts.algorithm.MinimumBoundingCircle
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel, Geometry}
import com.vividsolutions.jts.geom.{Envelope, Coordinate, Point, Polygon}
import com.vividsolutions.jts.io.WKTReader

import org.apache.commons.math3.geometry.euclidean.twod.DiskGenerator
import org.apache.commons.math3.geometry.euclidean.twod.Vector2D
import org.apache.commons.math3.geometry.enclosing.SupportBallGenerator

import scala.collection.JavaConverters._
import scala.io.Source

import org.slf4j.{Logger, LoggerFactory}

import edu.ucr.dblab.pflock.spmf.{Transactions, Transaction, AlgoLCM2}

import archery._

object Utils {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  /*** Case Class ***/
  case class Settings(
    epsilon_prime: Double = 10.0,
    mu: Int = 3,
    delta: Int = 5,
    capacity: Int = 1000,
    tolerance: Double = 1e-3,
    appId: String = "",
    tag: String = "",
    debug: Boolean = false
  ){
    val scale = 1 / tolerance
    val epsilon = epsilon_prime + tolerance
    val r = (epsilon_prime / 2.0) + tolerance
    val r2 = math.pow(epsilon_prime / 2.0, 2) + tolerance

    def info: String = s"$appId|$epsilon_prime|$mu|$delta"
  }

  case class STPoint(point: Point, cid: Int = 0){
    val userData = point.getUserData.asInstanceOf[String].split("\t")
    val oid = userData(0).toLong
    val tid = userData(1).toInt

    def distance(other: STPoint): Double = {
      point.distance(other.point)
    }

    def X: Double = point.getX

    def Y: Double = point.getY

    def toText: String = s"${point.toText()}"

    def wkt: String = s"${point.toText()}\t$cid\t$oid\t$tid"

    override def toString: String = s"${point.getX}\t${point.getY}\t$cid\t$oid\t$tid"
  }

  case class Disk(center: Point, pids: List[Int], support: List[Int] = List.empty[Int]){
    var subset: Boolean = false
    val X: Float = center.getX.toFloat
    val Y: Float = center.getY.toFloat
    
    def envelope: Envelope = center.getEnvelopeInternal

    def getExpandEnvelope(r: Double): Envelope = {
      val envelope = center.getEnvelopeInternal
      envelope.expandBy(r)
      envelope
    }

    def pidsSet: Set[Int] = pids.toSet

    def intersect(other: Disk): Set[Int] = this.pidsSet.intersect(other.pidsSet)

    def bbox(r: Double): Box =
      Box((X - r).toFloat, (Y - r).toFloat, (X + r).toFloat, (Y + r).toFloat)
  }

  case class Key(x: Int, y: Int)

  case class DataFiles(
    points:   List[Point],
    pairs:    List[(Point, Point)],
    centers:  List[Point],
    disks:    List[Disk],
    maximals: List[Disk]
  )

  /*** BFE Functions ***/
  def encode(x: Int, y: Int): Long = {
    if (x < y)
      (y + 1) * (y + 1) + (x + 1)
    else
      (x + 1) * (x + 1) + (y + 1)
  }

  def decode(z: Long): (Int, Int) = {
    val q = math.floor(math.sqrt(z.toDouble))
    val l = z - q * q
    (l.toInt - 1, q.toInt - 1)
  }

  def buildGrid(points: List[STPoint], epsilon: Double): Map[Long, List[STPoint]] = {
    val grid = points.map{ point =>
      val i = (point.X / epsilon).toInt
      val j = (point.Y / epsilon).toInt
      (encode(i, j), point)
    }.groupBy(_._1)

    grid.mapValues(_.map(_._2))
  }

  def findCenters(points: List[Point], epsilon: Double, r2: Double)
      (implicit geofactory: GeometryFactory, settings: Settings): List[Point] = {
    val centers = for {
      a <- points
      b <- points if {
        val id1 = a.getUserData.asInstanceOf[Int]
        val id2 = b.getUserData.asInstanceOf[Int]

        id1 < id2 & a.distance(b) <= epsilon
      }
    } yield {
      calculateCenterCoordinates(a, b, r2)
    }

    centers.flatten    
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
      //.filter(_.getItems.size >= mu)
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

  def calculateCenterCoordinates(p1: Point, p2: Point, r2: Double)
    (implicit geofactory: GeometryFactory, settings: Settings): List[Point] = {

    val X: Double = p1.getX - p2.getX
    val Y: Double = p1.getY - p2.getY
    val D2: Double = math.pow(X, 2) + math.pow(Y, 2)
    if (D2 != 0.0){
      val root: Double = math.sqrt(math.abs(4.0 * (r2 / D2) - 1.0))
      val h1: Double = ((X + Y * root) / 2) + p2.getX
      val k1: Double = ((Y - X * root) / 2) + p2.getY
      val h2: Double = ((X - Y * root) / 2) + p2.getX
      val k2: Double = ((Y + X * root) / 2) + p2.getY
      val h = geofactory.createPoint(new Coordinate(h1,k1))
      val k = geofactory.createPoint(new Coordinate(h2,k2))
      //val arr1 = p1.getUserData.asInstanceOf[String].split("\t")
      //val ids = List(, p2.getUserData.asInstanceOf[Int])
      //h.setUserData(ids)
      //k.setUserData(ids)
      List(h, k)
    } else {
      val p2_prime = geofactory.createPoint(new Coordinate(p2.getX + settings.tolerance,
        p2.getY))
      calculateCenterCoordinates(p1, p2_prime, r2)
    }
  }

  /*** Misc Functions ***/
  def readPoints(input: String, isWKT: Boolean = false)
    (implicit geofactory: GeometryFactory): List[STPoint] = {

    val buffer = Source.fromFile(input)
    val points = buffer.getLines.zipWithIndex.toList
      .map{ line =>
        if(isWKT){
          val reader = new WKTReader(geofactory)
          val id = line._2
          val t  = 0
          val point = reader.read(line._1).asInstanceOf[Point]

          point.setUserData(s"$id\t$t")
          STPoint(point)
        } else {
          val arr = line._1.split("\t")
          val id = arr(0).toInt
          val x = arr(1).toDouble
          val y = arr(2).toDouble
          val t = arr(3).toInt
          val point = geofactory.createPoint(new Coordinate(x, y))

          point.setUserData(s"$id\t$t")
          STPoint(point)
        }
      }
    buffer.close
    points
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

  def timer[R](msg: String)(block: => R): R = {
    val t0 = clocktime
    val result = block    // call-by-name
    val t1 = clocktime
    logger.info("TIME|%-30s|%6.2f".format(msg, (t1 - t0) / 1e9))
    result
  }

  def debug[R](block: => R)(implicit d: Boolean): Unit = { if(d) block }

  def save(filename: String)(content: Seq[String]): Unit = {
    val start = clocktime
    val f = new java.io.PrintWriter(filename)
    f.write(content.mkString(""))
    f.close
    val end = clocktime
    val time = "%.2f".format((end - start) / 1e9)
    println(s"Saved ${filename}\tin\t${time}s\t[${content.size} records].")
  }
}
