import org.apache.spark.storage.StorageLevel
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.datasyslab.geospark.spatialRDD.{PointRDD, CircleRDD}
import org.datasyslab.geospark.enums.{GridType, IndexType, FileDataSplitter}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence
import com.vividsolutions.jts.geom.{GeometryFactory, Geometry, Envelope, Coordinate, Polygon, LinearRing, Point}
import org.rogach.scallop._
import org.slf4j.{LoggerFactory, Logger}
import scala.collection.JavaConverters._
import java.io.PrintWriter

object BufferPartitioner{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory()
  var appID      = ""
  var grid       = ""
  var startTime  = clocktime
  var distance   = 0.0
  var executors  = 0
  var partitions = 0
  var finalPartitions = 0L

  def main(args: Array[String]) = {
    val params     = new BPConf(args)
    val input      = params.input()
    val output     = params.output()
    val precisio   = params.precision()
    val offset     = params.offset()
    val host       = params.host()
    val port       = params.port()
    val portUI     = params.portui()
    val customx    = params.customx()
    val customy    = params.customy()
    val index      = params.index()
    val cores      = params.cores()
    val debug      = params.debug()
    val local      = params.local()
    val indexType = index match {
      case "QUADTREE"  => IndexType.QUADTREE
      case "RTREE"     => IndexType.RTREE
    }
    grid           = params.grid()
    val gridType = grid match {
      case "QUADTREE"  => GridType.QUADTREE
      case "RTREE"     => GridType.RTREE
      case "EQUALGRID" => GridType.EQUALGRID
      case "KDBTREE"   => GridType.KDBTREE
      case "HILBERT"   => GridType.HILBERT
      case "VORONOI"   => GridType.VORONOI
      case "CUSTOM"    => GridType.CUSTOM
    }
    var master     = ""
    if(local){
      master = s"local[$cores]"
    } else {
      master = s"spark://${host}:${port}"
    }
    executors  = params.executors()
    distance   = params.distance()
    partitions = params.partitions()

    // Starting session...
    var timer = clocktime
    log("Session start", timer, 0, "START")
    val maxCores = cores * executors
    val spark = SparkSession.builder().
      config("spark.default.parallelism", 3 * cores * executors).
      config("spark.serializer",classOf[KryoSerializer].getName).
      config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).
      config("spark.scheduler.mode", "FAIR").
      master(master).appName("Scaleup").
      config("spark.cores.max", maxCores).
      config("spark.executor.cores", cores).
      getOrCreate()
    import spark.implicits._
    appID      = spark.sparkContext.applicationId
    startTime  = spark.sparkContext.startTime
    if(grid == "CUSTOM") { partitions = (customx * customy).toInt }
    log("Session start", timer, 0, "END")    

    // Reading data...
    timer = clocktime
    log("Input read", timer, 0, "START")
    val points = new PointRDD(spark.sparkContext, input, offset, FileDataSplitter.TSV, true)
    val nPoints = points.rawSpatialRDD.rdd.count()
    log("Input read", timer, nPoints, "END")

    // Partitioning...
    timer = clocktime
    log("Points and buffers partitioned", timer, 0, "START")
    points.analyze()
    if(grid == "CUSTOM"){
      points.setNumX(customx.toInt)
      points.setNumY(customy.toInt)
    }
    points.spatialPartitioning(gridType, partitions)
    points.spatialPartitionedRDD.cache()
    points.buildIndex(indexType, true)
    val buffer = new CircleRDD(points, distance)
    buffer.spatialPartitioning(points.getPartitioner)
    var bufferWKT = buffer.spatialPartitionedRDD.rdd
      .mapPartitionsWithIndex{ (i, buffers) =>
        val wkt = buffers.map(buffer => (i,buffer.getCenterGeometry()))
        wkt.toIterator
      }
    if(grid == "CUSTOM"){ bufferWKT =  bufferWKT.filter(b => b._1 != partitions) }
    buffer.spatialPartitionedRDD.cache()
    val nBuffer = buffer.spatialPartitionedRDD.count()
    logger.info(s"Number of geometries in buffer partitions: $nBuffer")
    val f = new java.io.PrintWriter(s"${output}/partitions_${grid}_${distance}.wkt")
    f.write(bufferWKT.map(b => s"${b._1}\t${b._2}\n").collect().mkString(""))
    f.close()
    val g = new java.io.PrintWriter(s"${output}/grids_${grid}_${distance}.wkt")
    val grids = buffer.getPartitioner.getGrids.asScala.map(envelope2Polygon)
    g.write(grids.map(g => s"${g.toText()}\n").mkString(""))
    g.close()
    finalPartitions = buffer.spatialPartitionedRDD.rdd.getNumPartitions
    log("1.Points and buffers partitioned", timer, finalPartitions, "END")

    // Closing session...
    timer = clocktime
    log("Session close", timer, 0, "START")
    spark.close()
    log("Session close", timer, 0, "END")
  }

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long = -1, status: String = "NULL"): Unit = {
    logger.info("%-24s|%-10s|%2d|%4.0f|%5d|%5d|%-40s|%6.2f|%6.2f|%6d|%s".format(appID, grid, executors, distance, partitions, finalPartitions, msg, (clocktime - startTime)/1000.0, (clocktime - timer)/1000.0, n, status))
  }

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
}

class BPConf(args: Seq[String]) extends ScallopConf(args) {
  val input:      ScallopOption[String]  = opt[String]  (required = true)
  val output:     ScallopOption[String]  = opt[String]  (default = Some("/home/acald013/Research/Validation/Scaleup/logs"))
  val host:       ScallopOption[String]  = opt[String]  (default = Some("169.235.27.138"))
  val port:       ScallopOption[String]  = opt[String]  (default = Some("7077"))
  val portui:     ScallopOption[String]  = opt[String]  (default = Some("4040"))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val executors:  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val distance:   ScallopOption[Double]  = opt[Double]  (default = Some(110.0))
  val precision:  ScallopOption[Double]  = opt[Double]  (default = Some(0.001))
  val grid:       ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val index:      ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val partitions:  ScallopOption[Int]     = opt[Int]     (default = Some(2))
  val customx:    ScallopOption[Int]     = opt[Int]     (default = Some(30))
  val customy:    ScallopOption[Int]     = opt[Int]     (default = Some(30))
  val offset:     ScallopOption[Int]     = opt[Int]     (default = Some(1))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
