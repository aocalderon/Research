import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import scala.collection.JavaConverters._
import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import com.vividsolutions.jts.operation.buffer.BufferParameters
import com.vividsolutions.jts.geom.{GeometryFactory, Geometry, Envelope, Coordinate, Polygon, LinearRing, Point}
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType}
import org.datasyslab.geospark.spatialRDD.PointRDD
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import scala.collection.mutable.ListBuffer
import java.io._

object DatasetReplicator {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory();
  private val precision: Double = 0.0001

  case class ST_Point(pid: Int, x: Double, y: Double, t: Int)

  def envelope2Polygon(e: Envelope): Polygon = {
    val minX = e.getMinX()
    val minY = e.getMinY()
    val maxX = e.getMaxX()
    val maxY = e.getMaxY()
    val p1 = new Coordinate(minX, minY)
    val p2 = new Coordinate(minX, maxY)
    val p3 = new Coordinate(maxX, maxY)
    val p4 = new Coordinate(maxX, minY)
    val coordArraySeq = new CoordinateArraySequence( Array(p1,p2,p3,p4,p1), 2)
    val ring = new LinearRing(coordArraySeq, geofactory)
    new Polygon(ring, null, geofactory)
  }

  def round3(n: Double): Double = { val s = 1000; (math round n * s) / s }

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }

  def getGridsByCellNumber(boundary: Envelope, x: Int, y: Int): List[Envelope] = {
    var envelops = new ListBuffer[Envelope]()
    val maxx = boundary.getMaxX
    val minx = boundary.getMinX
    val intervalX = (maxx - minx) / x
    val maxy = boundary.getMaxY
    val miny = boundary.getMinY
    val intervalY = (maxy - miny) / y
    for {
      i <- 0 until x
      j <- 0 until y
    } {
      envelops += new Envelope(
        round3(minx + intervalX * i),
        round3(minx + intervalX * (i + 1)) - precision,
        round3(miny + intervalY * j),
        round3(miny + intervalY * (j + 1)) - precision
      )
    }

    envelops.toList
  }

  def getGridWidth(grid: List[Envelope]): Double = {
    val minx = grid.map(_.getMinX).reduceLeft(_ min _)
    val maxx = grid.map(_.getMaxX).reduceLeft(_ max _)
    maxx - minx
  }

  def savePoints(points: RDD[ST_Point], filename: String): Unit = {
    val f = new PrintWriter(filename)
    f.write(points.map(p => s"${p.pid}\t${p.x}\t${p.y}\t${p.t}\n").collect.mkString(""))
    f.close()
  }

  def saveGrid(grid: List[Envelope], filename: String): Unit = {
    val f = new PrintWriter(filename)
    f.write(grid.map(e => s"${envelope2Polygon(e).toText()}\n").mkString(""))
    f.close()
  }

  def main(args: Array[String]): Unit = {
    val params = new DRConf(args)
    val input  = params.input()
    val name   = params.name()
    val offset = params.offset()
    val frame  = params.frame()
    val times  = params.times()

    val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .master("local[*]").getOrCreate()
    import spark.implicits._
    logger.info("Session started")

    val filename = params.input()
    val points = new PointRDD(spark.sparkContext, filename, offset, FileDataSplitter.TSV, true)
    points.analyze()
    val max_pid = points.rawSpatialRDD.rdd.map(_.getUserData.toString().split("\t")(0).toInt).max()
    logger.info("Data read")

    val fullBoundary = points.boundary()
    fullBoundary.expandBy(frame)
    val grid1x = params.grid1x()
    val grid1y = params.grid1y()
    val grid1  = getGridsByCellNumber(fullBoundary, grid1x, grid1y)
    val nGrid1 = grid1.size
    logger.info(s"Number of cells in Grid 1: $nGrid1")

    val grid2x = params.grid2x()
    val grid2y = params.grid2y()
    val grid2  = getGridsByCellNumber(fullBoundary, grid2x, grid2y)
    val nGrid2 = grid2.size
    logger.info(s"Number of cells in Grid 2: $nGrid2")

    val pid_adder = scala.math.pow(10, max_pid.toString.size).toInt
    var data = new scala.collection.mutable.ListBuffer[(RDD[ST_Point], List[Envelope], List[Envelope])]()
    var pointsRDD = points.rawSpatialRDD.rdd.map{ p =>
      val arr = p.getUserData.toString.split("\t")
      val pid = arr(0).toInt
      val t   = arr(1).toInt
      val x   = p.getX
      val y   = p.getY
      new ST_Point(pid, x, y, t)
    }
    val default = (pointsRDD, grid1, grid2)
    data += default
    var x_adder = getGridWidth(grid1)
    for( t <- 1 until times ){
      val p  = default._1.map(i => new ST_Point(i.pid + (t * pid_adder), i.x + (t * x_adder), i.y, i.t))
      val g1 = default._2.map(e => new Envelope(e.getMinX + (t * x_adder), e.getMaxX + (t * x_adder), e.getMinY, e.getMaxY))
      val g2 = default._3.map(e => new Envelope(e.getMinX + (t * x_adder), e.getMaxX + (t * x_adder), e.getMinY, e.getMaxY))
      val entry = (p, g1, g2) 
      data += entry
      x_adder = getGridWidth(g1)
    }

    val path = input.split("/").reverse.tail.reverse.mkString("/")
    val d = data.toList
    savePoints(d.map(_._1).reduce(_ union _), s"${path}/${name}${times}_points.wkt")
    saveGrid(d.map(_._2).reduce(_ ++ _),      s"${path}/${name}${times}_grid-${times * grid1x}x${grid1y}.wkt") 
    saveGrid(d.map(_._3).reduce(_ ++ _),      s"${path}/${name}${times}_grid-${times * grid2x}x${grid2y}.wkt") 

    spark.close()
    logger.info("Session closed")
  }
}

class DRConf(args: Seq[String]) extends ScallopConf(args) {
  val input:  ScallopOption[String]   = opt[String]  (required = true)
  val name:   ScallopOption[String]   = opt[String]  (required = true)
  val output: ScallopOption[String]   = opt[String]  (default  = Some("/tmp/output.tsv"))
  val offset: ScallopOption[Int]      = opt[Int]     (default  = Some(1))
  val times:  ScallopOption[Int]      = opt[Int]     (default  = Some(2))
  val grid1x: ScallopOption[Int]      = opt[Int]     (default  = Some(9))
  val grid1y: ScallopOption[Int]      = opt[Int]     (default  = Some(6))
  val grid2x: ScallopOption[Int]      = opt[Int]     (default  = Some(15))
  val grid2y: ScallopOption[Int]      = opt[Int]     (default  = Some(15))
  val frame:  ScallopOption[Double]   = opt[Double]  (default  = Some(1.0))
  val debug:  ScallopOption[Boolean]  = opt[Boolean] (default  = Some(false))

  verify()
}
