import com.vividsolutions.jts.geom.{Point, GeometryFactory, Coordinate}
import org.geotools.geometry.jts.JTSFactoryFinder
import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.spatialOperator.{JoinQuery, KNNQuery, RangeQuery}
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD, PolygonRDD}
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator

object Tester{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geometryFactory: GeometryFactory = JTSFactoryFinder.getGeometryFactory(null);
  private val precision: Double = 0.001

  def main(args: Array[String]) = {
    val params = new TesterConf(args)
    val debug: Boolean  = params.debug()
    val epsilon: Double = params.epsilon()
    val input:String    = params.input()

    // Starting session...
    var timer = System.currentTimeMillis()
    val conf = new SparkConf().setAppName("GeoSparkTester").setMaster("local[*]")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
    val sc = new SparkContext(conf)
    log("Session started", timer)

    // Reading data...
    timer = System.currentTimeMillis()
    val points = new PointRDD(sc, input, 1, FileDataSplitter.TSV, true, StorageLevel.MEMORY_ONLY)
    points.CRSTransform("epsg:3068", "epsg:3068")
    val nPoints = points.rawSpatialRDD.count()
    log("Data read", timer, nPoints)

    // Finding pairs...
    timer = System.currentTimeMillis()
    val buffer = new CircleRDD(points, epsilon + precision)
    points.analyze()
    buffer.analyze()
    buffer.spatialPartitioning(GridType.QUADTREE)
    points.spatialPartitioning(buffer.getPartitioner)
    points.buildIndex(IndexType.QUADTREE, true)
    points.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
    buffer.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

    val considerBoundary = true
    val usingIndex = true
    val pairs = JoinQuery.DistanceJoinQueryFlat(points, buffer, usingIndex, considerBoundary)
      .rdd.map{ pair =>
        val id1 = pair._1.getUserData().toString().split("\t").head.trim().toInt
        val p1  = pair._1.getCentroid
        val id2 = pair._2.getUserData().toString().split("\t").head.trim().toInt
        val p2  = pair._2
        ( (id1, p1) , (id2, p2) )
      }.filter(p => p._1._1 < p._2._1)
    val nPairs = pairs.count()
    log("Pairs found", timer, nPairs)

    // Finding centers...
    val r2: Double = math.pow(epsilon / 2.0, 2)
    val centers = pairs.map{ p =>
        val p1 = p._1._2
        val p2 = p._2._2
        calculateCenterCoordinates(p1, p2, r2)
      }
    
    val nCenters = centers.count()
    log("Centers found", timer, nCenters)

    // Closing session...
    timer = System.currentTimeMillis()
    sc.stop()
    log("Session closed", timer)
  }

  def calculateCenterCoordinates(p1: Point, p2: Point, r2: Double): (Point, Point) = {
    var h = geometryFactory.createPoint(new Coordinate(-1.0,-1.0))
    var k = geometryFactory.createPoint(new Coordinate(-1.0,-1.0))
    val X: Double = p1.getX - p2.getX
    val Y: Double = p1.getY - p2.getY
    val D2: Double = math.pow(X, 2) + math.pow(Y, 2)
    if (D2 != 0.0){
      val root: Double = math.sqrt(math.abs(4.0 * (r2 / D2) - 1.0))
      val h1: Double = ((X + Y * root) / 2) + p2.getX
      val k1: Double = ((Y - X * root) / 2) + p2.getY
      val h2: Double = ((X - Y * root) / 2) + p2.getX
      val k2: Double = ((Y + X * root) / 2) + p2.getY
      h = geometryFactory.createPoint(new Coordinate(h1,k1))
      k = geometryFactory.createPoint(new Coordinate(h2,k2))
    }
    (h, k)
  } 

  def log(msg: String, timer: Long, n: Long = 0, tag: String = ""): Unit ={
    if(n == 0)
      logger.info("%-50s|%6.2f".format(msg,(System.currentTimeMillis()-timer)/1000.0))
    else
      logger.info("%-50s|%6.2f|%6d|%s".format(msg,(System.currentTimeMillis()-timer)/1000.0,n,tag))
  }
}
