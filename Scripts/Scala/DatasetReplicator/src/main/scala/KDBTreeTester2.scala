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
import org.datasyslab.geospark.spatialPartitioning.{KDBTree, KDBTreePartitioner}
import scala.collection.mutable.ListBuffer
import java.io._

object KDBTreeTester2 {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory();
  private val reader = new com.vividsolutions.jts.io.WKTReader(geofactory)
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

  def readGridCell(wkt: String): Envelope = {
    reader.read(wkt).getEnvelopeInternal
  }

  def main(args: Array[String]): Unit = {
    val params   = new KT2Conf(args)
    val input    = params.input()
    val name     = params.name()
    val offset   = params.offset()
    val frame    = params.frame()
    val fraction = params.fraction()

    val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .master("local[*]").getOrCreate()
    import spark.implicits._
    logger.info("Session started")

    val filename = params.input()
    val points = new PointRDD(spark.sparkContext, filename, offset, FileDataSplitter.TSV, true)
    points.analyze()
    logger.info("Data read")

    val fullBoundary = points.boundary()
    fullBoundary.expandBy(frame)
    val levels = params.levels()
    val entries = params.entries()

    val tree = new KDBTree(1, 1, fullBoundary)
    import scala.io.Source
    val cells = Source.fromFile("/tmp/B1_kdbtree.wkt").getLines.toList.map(readGridCell)
    tree.setChildren(cells.asJava)
    tree.assignLeafIds()
    val partitioner = new KDBTreePartitioner(tree)

    points.spatialPartitioning(partitioner)

    val grid = points.getPartitioner().getGrids().asScala.toList
    saveGrid(grid, s"/tmp/${name}_kdbtree.wkt")
    logger.info("KDBTree saved")

    spark.close()
    logger.info("Session closed")
  }
}

class KT2Conf(args: Seq[String]) extends ScallopConf(args) {
  val input:   ScallopOption[String]   = opt[String]  (required = true)
  val name:    ScallopOption[String]   = opt[String]  (required = true)
  val output:  ScallopOption[String]   = opt[String]  (default  = Some("/tmp/output.tsv"))
  val offset:  ScallopOption[Int]      = opt[Int]     (default  = Some(1))
  val levels:  ScallopOption[Int]      = opt[Int]     (default  = Some(5))
  val entries: ScallopOption[Int]      = opt[Int]     (default  = Some(2000))
  val frame:   ScallopOption[Double]   = opt[Double]  (default  = Some(1.0))
  val fraction:ScallopOption[Double]   = opt[Double]  (default  = Some(0.25))
  val debug:   ScallopOption[Boolean]  = opt[Boolean] (default  = Some(false))

  verify()
}
