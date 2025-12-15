package puj

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.scala.Logging

import streaminer.{MurmurHash3 => Murmur, SpookyHash32 => Spooky}

import org.locationtech.jts.geom._

import scala.util.Random
import scala.collection.mutable
import scala.collection.mutable.BitSet

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
    val oid = -1
    val tid = -1
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

    def getCircleWTK(implicit S: Settings): String = s"${center.buffer(S.r, 25).toText}\t$X\t$Y\t[$data]\t$pidsText"

    def equals(other: Disk): Boolean = this.pidsText == other.pidsText

    def compare(other: Disk): Int = this.center.getCoordinate.compareTo(other.center.getCoordinate)

    def duplicates(tree: RTree[Disk])(implicit settings: Settings): Seq[Disk] = tree
      .search(this.bbox(settings.epsilon.toFloat))
      .map(_.value)
      .filter(_.equals(this))
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
}
