//**
package edu.ucr.dblab

import org.slf4j.{LoggerFactory, Logger}
import scala.collection.JavaConverters._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, CircleRDD, PointRDD}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.enums.{GridType, IndexType}
import com.vividsolutions.jts.geom.{Envelope, Coordinate, Point, Polygon}
import com.vividsolutions.jts.geom.GeometryFactory
import org.datasyslab.geospark.spatialPartitioning.quadtree._
import org.datasyslab.geospark.spatialPartitioning.QuadtreePartitioning
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import scala.io.Source
import edu.ucr.dblab.Utils._
import org.rogach.scallop._

//**
object PartitionerTester {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class PointWKT(wkt: String, id: Int, t: Int)
  case class ST_Point(id: Int, x: Double, y: Double, t: Int){
    def asWKT = PointWKT(s"POINT($x $y)", id, t)
  }

  def main(args: Array[String]): Unit = {
    implicit val params = new PTConf(args)
    implicit val geofactory = new GeometryFactory()    
    implicit val spark = SparkSession.builder()
      .appName("PartitionerTester")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).getOrCreate()
    GeoSparkSQLRegistrator.registerAll(spark)
    import spark.implicits._

    implicit val conf = spark.sparkContext.getConf
    def getConf(property: String)(implicit conf: SparkConf): String = conf.get(property)
    def appId: String = if(getConf("spark.master").contains("local")){
      getConf("spark.app.id")
    } else {
      getConf("spark.app.id").takeRight(4)
    }
    def header(msg: String): String = s"GeoTesterRDD|${appId}|$msg|Time"
    def n(msg:String, count: Long): Unit = {
      logger.info(s"GeoTesterRDD|${appId}|$msg|Load|$count")
    }
    case class Grid(id: Int, lineage: String, envelope: Envelope)
    val (partitioner, samples) = timer{"Reading partitions"}{
      val envelopes = Source.fromFile("/tmp/envelopes.tsv")
      val grids = envelopes.getLines.map{ line =>
        val arr = line.split("\t")
        val id = arr(0).toInt
        val lineage = arr(1)
        val envelope = new Envelope(arr(2).toDouble, arr(4).toDouble, arr(3).toDouble, arr(5).toDouble)
        Grid(id, lineage, envelope)
      }.toList
      val samples = grids.sortBy(_.lineage.size).map(_.envelope)
      val partitioner = new CustomPartitioner(GridType.QUADTREE, samples.asJava)
      envelopes.close

      (partitioner, samples)
    }

    val (pointsRDD, nPointsRDD, envelope) = timer{"Reading data"}{
      val pointsSchema = ScalaReflection.schemaFor[ST_Point].dataType.asInstanceOf[StructType]
      val pointsRaw = spark.read.schema(pointsSchema)
        .option("delimiter", "\t").option("header", false)
        .csv(params.input()).as[ST_Point]
        .repartition(samples.size)
        .rdd
      val pointsRDD = new SpatialRDD[Point]
      val points = pointsRaw.map{ point =>
        val userData = s"${point.id}\t${point.t}"
        val p = geofactory.createPoint(new Coordinate(point.x, point.y))
        p.setUserData(userData)
        p
      }
      pointsRDD.setRawSpatialRDD(points)
      pointsRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY)
      val nPointsRDD = pointsRDD.rawSpatialRDD.count()
      pointsRDD.analyze()
      n("Data", nPointsRDD)
      (pointsRDD, nPointsRDD, pointsRDD.boundaryEnvelope)
    }

    val distance = params.epsilon() + params.precision()
    val stageA = "Partitions done"
    val (points, buffers, npartitions) = timer{stageA}{
      pointsRDD.spatialPartitioning(partitioner)
      pointsRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
      val npartitions = pointsRDD.getPartitioner.getGrids.size()
      val buffersRDD = new CircleRDD(pointsRDD, distance)
      buffersRDD.analyze(envelope, nPointsRDD.toInt)
      buffersRDD.spatialPartitioning(pointsRDD.getPartitioner)
      buffersRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
      n(stageA, buffersRDD.spatialPartitionedRDD.count())
      (pointsRDD, buffersRDD, npartitions)
    }

    // Finding pairs and centers...
    def calculateCenterCoordinates(p1: Point, p2: Point, r2: Double): (Point, Point) = {
      var h = geofactory.createPoint(new Coordinate(-1.0,-1.0))
      var k = geofactory.createPoint(new Coordinate(-1.0,-1.0))
      val X: Double = p1.getX - p2.getX
      val Y: Double = p1.getY - p2.getY
      val D2: Double = math.pow(X, 2) + math.pow(Y, 2)
      if (D2 != 0.0){
        val root: Double = math.sqrt(math.abs(4.0 * (r2 / D2) - 1.0))
        val h1: Double = ((X + Y * root) / 2) + p2.getX
        val k1: Double = ((Y - X * root) / 2) + p2.getY
        val h2: Double = ((X - Y * root) / 2) + p2.getX
        val k2: Double = ((Y + X * root) / 2) + p2.getY
        h = geofactory.createPoint(new Coordinate(h1,k1))
        k = geofactory.createPoint(new Coordinate(h2,k2))
      }
      (h, k)
    }
    val r2: Double = math.pow(params.epsilon() / 2.0, 2)
    val considerBoundary = true
    val stageB = "A.Pairs and centers found"
    val (centers, nCenters) = timer{header(stageB)}{
      val usingIndex = false
      val centersPairs = JoinQuery.DistanceJoinQueryFlat(points, buffers, usingIndex, considerBoundary)
        .rdd.map{ pair =>
          val id1 = pair._1.getUserData().toString().split("\t").head.trim().toInt
          val p1  = pair._1.getCentroid
          val id2 = pair._2.getUserData().toString().split("\t").head.trim().toInt
          val p2  = pair._2
          ( (id1, p1) , (id2, p2) )
        }.filter(p => p._1._1 < p._2._1).map{ p =>
        val p1 = p._1._2
        val p2 = p._2._2
        calculateCenterCoordinates(p1, p2, r2)
      }.persist(StorageLevel.MEMORY_ONLY)
      val centers = centersPairs.map(_._1)
        .union(centersPairs.map(_._2))
        .persist(StorageLevel.MEMORY_ONLY)
      val nCenters = centers.count()
      n(stageB, nCenters)
      (centers, nCenters)
    }

    spark.close
  }
}

class PTConf(args: Seq[String]) extends ScallopConf(args) {
  val input = opt[String](default = Some(""))
  val epsilon = opt[Double](default = Some(10.0))
  val mu = opt[Int](default = Some(2))
  val precision = opt[Double](default = Some(0.001))
  val partitions = opt[Int](default = Some(256))
  val parallelism = opt[Int](default = Some(324))
  val gridtype = opt[String](default = Some("quadtree"))
  val indextype = opt[String](default = Some("none"))

  verify()
}
