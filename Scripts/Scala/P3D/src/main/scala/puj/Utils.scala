package puj

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.scala.Logging


import archery.{RTree, Box}
import streaminer.{MurmurHash3 => Murmur, SpookyHash32 => Spooky}

import org.locationtech.jts.geom._
import org.locationtech.jts.index.strtree.STRtree
import org.locationtech.jts.io.WKTReader

import scala.io.Source
import scala.util.Random
import scala.collection.mutable
import scala.collection.mutable.BitSet

import edu.ucr.dblab.pflock.sedona.quadtree.Quadtree.Cell

object Utils extends Logging {

  // Case Classes...

  case class Data(oid: Int, tid: Int)

  case class SimplePartitioner(partitions: Int) extends Partitioner {
    override def numPartitions: Int          = partitions
    override def getPartition(key: Any): Int = key.asInstanceOf[Int]
  }

  case class Settings(
    dataset: String = "",
    epsilon_prime: Double = 10.0,
    mu: Int = 3,
    delta: Int = 5,
    step: Int = 1,
    sdist: Double = 20.0,
    endtime: Int = 10,
    capacity: Int = 200,
    fraction: Double = 0.01,
    tolerance: Double = 1e-3,
    density: Double = 1000.0,
    tag: String = "",
    method: String = "PFlock",
    debug: Boolean = false,
    cached: Boolean = false,
    tester: Boolean = false,
    saves: Boolean = false,
    print: Boolean = false,
    iindex: Boolean = true,
    output: String = "/tmp/"
  ){
    val scale: Double = 1 / tolerance
    val epsilon: Double = epsilon_prime + tolerance
    val r: Double = (epsilon_prime / 2.0) + tolerance
    val r2: Double = math.pow(epsilon_prime / 2.0, 2) + tolerance
    val expansion: Double = epsilon_prime * 1.5 + tolerance
    val dataset_name: String = {
      val d = dataset.split("/").last.split("\\.").head
      if(d.startsWith("part-")) d.split("-")(1) else d
    }

    var partitions: Int = 1

    def info: String = s"$partitions|$dataset_name|$epsilon_prime|$mu|$delta|$method"
  }

  case class STPoint(point: Point, cid: Int = 0){
    val userData = if(point.getUserData.isInstanceOf[Data]) point.getUserData.asInstanceOf[Data] else null
    val oid = if(point.getUserData.isInstanceOf[Data]) userData.oid else -1
    val tid = if(point.getUserData.isInstanceOf[Data]) userData.tid else -1
    var count = 0

    def envelope: Envelope = point.getEnvelopeInternal

    def distance(other: STPoint): Double = point.distance(other.point)

    def distanceToPoint(p: Point): Double = point.distance(p)
    
    def X: Double = point.getX

    def Y: Double = point.getY

    def getNeighborhood(points: List[STPoint])(implicit P: Params): List[STPoint] = {
      for{
        point <- points if{ this.distance(point) <= P.epsilon }
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

    override def toString: String = s"$oid\t${point.getX}\t${point.getY}\t$tid\t$cid\t$count\n"

    def archeryEntry: archery.Entry[STPoint] = {
      val x: Float = X.toFloat
      val y: Float = Y.toFloat
      archery.Entry(archery.Point(x, y), this)
    }

  }

  def NullPoint: STPoint = {
    val G: GeometryFactory = new GeometryFactory()
    STPoint(G.createPoint())
  }

  case class Disk(center: Point, pids: List[Int],
    start: Int = 0, end: Int = 0) extends Ordered [Disk]{

    var id = -1
    var locations: List[Coordinate] = List(center.getCoordinate)
    var lineage: String = ""
    var did: Int = -1
    var dids: List[Int] = List(-1)
    var subset: Boolean = false
    var data: String = try {
      center.getUserData.toString
    }
    catch {
      case e: java.lang.NullPointerException => "NoData"
    }

    val X: Float = center.getX.toFloat
    val Y: Float = center.getY.toFloat
    val count: Int = pids.size
    val pidsText = pids.sorted.mkString(" ")
    val SIG_SIZE = 128

    val signature: BitSet = {
      val signature_prime: BitSet = new BitSet
      pids.foreach{ oid =>
        pureHash(signature_prime, oid)
      }
      signature_prime
    }

    private def printHashInfo(name: String, oid: Int, value: Long, position: Long): Unit = {
      val soid = s"Hash: $name($oid) = "
      val sval = s" value: $value"
      val spos = s" position: $position"
      println(f"${soid}%-20s\t${sval}%-20s\t${spos}%-20s")
    }

    private def pureHash(signature: BitSet, oid: Int, size: Int = SIG_SIZE, seed: Int = 0, debug: Boolean = false): Unit = {
      val murmur_value = math.abs( Murmur.hashInt2(oid, seed) )
      val spooky_value = math.abs( Spooky.hash32(oid, seed) )
      val murmur_pos = murmur_value % size
      val spooky_pos = spooky_value % size
      if(debug){
        printHashInfo("murmur", oid, murmur_value, murmur_pos)
        printHashInfo("spooky", oid, spooky_value, spooky_pos)
      }
      signature(murmur_pos.toInt) = true
      signature(spooky_pos.toInt) = true
    }

    private def toBinaryString(bs: BitSet) = {
      val sb = new mutable.StringBuilder(SIG_SIZE)
      for (i <- SIG_SIZE - 1 to 0 by -1) {
        val bit = if( bs(i) ) 1 else 0
        sb.append(bit)
      }
      sb.reverse.toString
    }

    def toBinarySignature: String = toBinaryString(signature)

    def &(other: Disk): Boolean = {
      val r = this.signature & other.signature
      r == this.signature
    }

    def envelope: Envelope = new Envelope(center.getEnvelopeInternal)

    def getExpandEnvelope(r: Double): Envelope = {
      val envelope = new Envelope(center.getEnvelopeInternal)
      envelope.expandBy(r)
      envelope
    }

    def pidsSet: Set[Int] = pids.toSet

    def intersect(other: Disk): Set[Int] = this.pidsSet.intersect(other.pidsSet)

    def bbox(r: Float): Box = Box(X - r, Y - r, X + r, Y + r)

    def archeryEntry(implicit S: Settings): archery.Entry[Disk] = archery.Entry(this.bbox(S.r.toFloat), this)
    def pointEntry: archery.Entry[Disk] = archery.Entry(archery.Point(this.X, this.Y), this)

    def containedBy(cell: Cell): Boolean = cell.contains(this)

    def distance(other: Disk): Double = center.distance(other.center)

    def isSubsetOf(other: Disk): Boolean = pidsSet.subsetOf(other.pidsSet)

    override def toString: String = s"$start\t$end\t${pids.sorted.mkString(" ")}"

    def wkt: String = s"${center.toText}\t$start\t$end\t$pidsText"

    def getCircleWTK(implicit P: Params): String = s"${center.buffer(P.r, 25).toText}\t$X\t$Y\t[$data]\t$pidsText"

    def equals(other: Disk): Boolean = this.pidsText == other.pidsText

    def compare(other: Disk): Int = this.center.getCoordinate.compareTo(other.center.getCoordinate)

    def duplicates(tree: RTree[Disk])(implicit settings: Settings): Seq[Disk] = tree
      .search(this.bbox(settings.epsilon.toFloat))
      .map(_.value)
      .filter(_.equals(this))
  }


case class Stats(var nPoints: Int = 0, var nPairs: Int = 0, var nCenters: Int = 0,
    var nCandidates: Int = 0, var nMaximals: Int = 0,
    var nCliques: Int = 0, var nMBC: Int = 0, var nBoxes: Int = 0,
    var tCounts: Double = 0.0, var tRead: Double = 0.0, var tGrid: Double = 0.0,
    var tCliques: Double = 0.0, var tMBC: Double = 0.0,
    var tBand: Double = 0.0, var tSort: Double = 0.0,
    var tPairs: Double = 0.0, var tCenters: Double = 0.0,
    var tCandidates: Double = 0.0, var tMaximals: Double = 0.0,
    var tBoxes: Double = 0.0, var tFilter: Double = 0.0){

    def print(printTotal: Boolean = true): Unit = {
      log(s"Points     |${nPoints}")
      log(s"Pairs      |${nPairs}")
      log(s"Centers    |${nCenters}")
      log(s"Candidates |${nCandidates}")
      log(s"Maximals   |${nMaximals}")
      logt(s"Count     |${tCounts}")
      logt(s"Grid      |${tGrid}")
      logt(s"Read      |${tRead}")
      logt(s"Pairs     |${tPairs}")
      logt(s"Centers   |${tCenters}")
      logt(s"Candidates|${tCandidates}")
      logt(s"Maximals  |${tMaximals}")
      if(printTotal){
        val tTotal = tMaximals + tCandidates + tCenters + tPairs + tCliques + tRead + tGrid + tCounts
        logt(s"Total     |${tTotal}")
      }
    }

    def bfe_total(): Double = tMaximals + tCandidates + tCenters + tPairs + tCliques + tRead + tGrid + tCounts
    def psi_total(): Double = tBand + tSort + tPairs + tCenters + tCandidates + tBoxes + tFilter

    def printPSI(printTotal: Boolean = true): Unit = {
      log(s"Points     |$nPoints")
      log(s"Pairs      |$nPairs")
      log(s"Centers    |$nCenters")
      log(s"Candidates |$nCandidates")
      log(s"Boxes      |$nBoxes")
      log(s"Maximals   |$nMaximals")
      logt(s"Band      |$tBand")
      logt(s"Sort      |$tSort")
      logt(s"Pairs     |$tPairs")
      logt(s"Centers   |$tCenters")
      logt(s"Candidates|$tCandidates")
      logt(s"Boxes     |$tBoxes" )
      logt(s"Filter    |$tFilter")
      if (printTotal) {
        val tTotal = tBand + tSort + tPairs + tCenters + tCandidates + tBoxes + tFilter
        logger.info(s"Total     |${tTotal}")
      }
    }
  }

  // Methods...

  def gaussianSeries(
      n: Int = 1000,
      min: Double = 0.0,
      max: Double = 1.0
  ): List[Double] = {
    val data = (0 to n).toList.map(_ => Random.nextGaussian())
    rescaleList(data, min, max)
  }

  def rescaleList(
      data: List[Double],
      newMin: Double,
      newMax: Double
  ): List[Double] = {
    if (data.isEmpty) return List.empty
    val currentMin = data.min
    val currentMax = data.max
    if (currentMin == currentMax) {
      return data.map(_ => newMin)
    }
    data.map { x =>
      val normalized = (x - currentMin) / (currentMax - currentMin)
      normalized * (newMax - newMin) + newMin
    }
  }

  def generateGaussianPointset(
      n: Int,
      x_limit: Double = 5000.0,
      y_limit: Double = 5000.0,
      t_limit: Double = 1000.0
  )(implicit G: GeometryFactory, spark: SparkSession): RDD[Point] = {

    val Xs     = gaussianSeries(n, 0.0, x_limit)
    val Ys     = gaussianSeries(n, 0.0, y_limit)
    val Ts     = gaussianSeries(n, 0.0, t_limit).map(_.toInt)
    val points = (0 to n).map { i =>
      val x     = Xs(i)
      val y     = Ys(i)
      val t     = Ts(i)
      val point = G.createPoint(new Coordinate(x, y))
      point.setUserData(Data(i, t))
      point
    }

    saveAsTSV(
      "/tmp/P.wkt",
      points.map { point =>
        val x   = point.getX
        val y   = point.getY
        val t   = point.getUserData().asInstanceOf[Data].tid
        val i   = point.getUserData().asInstanceOf[Data].oid
        val wkt = point.toText()

        f"$i%d\t$x%.3f\t$y%.3f\t$t%d\t$wkt\n"
      }.toList
    )

    spark.sparkContext.parallelize(points)
  }

  def saveAsTSV(filename: String, content: Seq[String]): Unit = {
    import java.io._
    val pw = new PrintWriter(new File(filename))
    pw.write(content.mkString(""))
    pw.close()
    logger.info(s"Saved ${content.size} records to $filename")
  }

  def clocktime: Long = System.nanoTime()

  def log(msg: String): Unit = {
    logger.info(s"$msg")
  }

  def logt(msg: String): Unit = {
    log(msg)
  }

  def debug[R](block: => R)(implicit P: Params): Unit = { if(P.debug()) block }

  def save(filename: String)(content: Seq[String]): Unit = {
    val start = clocktime
    val f = new java.io.PrintWriter(filename)
    f.write(content.mkString(""))
    f.close
    val end = clocktime
    val time = "%.2f".format((end - start) / 1e9)
    println(s"Saved ${filename}\tin\t${time}s\t[${content.size} records].")
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

  def computeCentres(p1: STPoint, p2:STPoint)
    (implicit G: GeometryFactory, P: Params): List[Point] = {

    calculateCenterCoordinates(p1.point, p2.point).map{ centre =>
      centre.setUserData(s"${p1.oid} ${p2.oid}")
      centre
    }
  }

  def calculateCenterCoordinates(p1: Point, p2: Point)
    (implicit G: GeometryFactory, P: Params): List[Point] = {

    val X: Double = p1.getX - p2.getX
    val Y: Double = p1.getY - p2.getY
    val D2: Double = math.pow(X, 2) + math.pow(Y, 2)
    if (D2 != 0.0){
      val root: Double = math.sqrt(math.abs(4.0 * (P.r2 / D2) - 1.0))
      val h1: Double = ((X + Y * root) / 2) + p2.getX
      val k1: Double = ((Y - X * root) / 2) + p2.getY
      val h2: Double = ((X - Y * root) / 2) + p2.getX
      val k2: Double = ((Y + X * root) / 2) + p2.getY
      val h = G.createPoint(new Coordinate(h1,k1))
      val k = G.createPoint(new Coordinate(h2,k2))

      List(h, k)
    } else {
      val p2_prime = G.createPoint(new Coordinate(p2.getX + P.tolerance(), p2.getY))
      calculateCenterCoordinates(p1, p2_prime)
    }
  }

  def readPoints(input: String, isWKT: Boolean = false)
    (implicit geofactory: GeometryFactory, P: Params): List[STPoint] = {

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

          val data = Data(i, t)
          point.setUserData(data)
          STPoint(point)
        }
      }
    buffer.close

    points
  }
}
