package edu.ucr.dblab

import org.slf4j.{LoggerFactory, Logger}
import org.rogach.scallop._
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
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import com.vividsolutions.jts.geom.{Envelope, Coordinate, Point}
import com.vividsolutions.jts.geom.GeometryFactory
import edu.ucr.dblab.Utils._

object GeoTesterRDD{
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class PointWKT(wkt: String, id: Int, t: Int)

  case class ST_Point(id: Int, x: Double, y: Double, t: Int){
    def asWKT = PointWKT(s"POINT($x $y)", id, t)
  }

  def n(count: Long, name: String = ""): Unit = logger.info(s"$name: $count")

  def main(args: Array[String]): Unit = {
    logger.info("Starting session...")
    implicit val params = new GeoTesterConf(args)
    implicit val geofactory = new GeometryFactory()
    implicit val spark = SparkSession.builder()
      .appName(s"GeoTesterRDD ${params.partitions()} ${params.dpartitions()} ${params.gridtype()} ${params.indextype()}")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    GeoSparkSQLRegistrator.registerAll(spark)
    import spark.implicits._
    val indextype = params.indextype() match {
      case "rtree"    => Some(IndexType.RTREE)
      case "quadtree" => Some(IndexType.QUADTREE)
      case _ => None
    }
    val gridtype = params.gridtype() match {
      case "kdbtree"  => GridType.KDBTREE
      case "quadtree" => GridType.QUADTREE
    }
    implicit val conf = spark.sparkContext.getConf
    logger.info(s"${appId}|${System.getProperty("sun.java.command")}")
    def getConf(property: String)(implicit conf: SparkConf): String = conf.get(property)
    def appId: String = if(getConf("spark.master").contains("local")){
      getConf("spark.app.id")
    } else {
      getConf("spark.app.id").takeRight(4)
    }
    def header(msg: String): String = s"GeoTesterRDD|${appId}|$msg"

    logger.info("Starting session... Done!")

    val dpartitions = params.dpartitions()
    val envelope = new Envelope(1963862.582, 1995915.392, 533608.982, 574098.423)
    val (pointsRDD, nPointsRDD) = timer{"Reading data"}{
      val pointsSchema = ScalaReflection.schemaFor[ST_Point].dataType.asInstanceOf[StructType]
      val pointsRaw = spark.read.schema(pointsSchema)
        .option("delimiter", "\t").option("header", false)
        .csv(params.input()).as[ST_Point]
        .repartition(dpartitions)
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
      pointsRDD.analyze(envelope, nPointsRDD.toInt)
      n(nPointsRDD, "PointsRDD")
      (pointsRDD, nPointsRDD)
    }

    val distance = params.epsilon() + params.precision()
    val (points, buffers, usingIndex) = timer{header("A.Partitions done")}{
      pointsRDD.spatialPartitioning(gridtype, params.partitions())
      val usingIndex =  indextype match {
        case Some(index) => {
          pointsRDD.buildIndex(index, true)
          pointsRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
          logger.info(s"IndexType: ${index.name()}.")
          true
        }
        case None => {
          pointsRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
          logger.info("IndexType: None.")
          false
        }
      }
      val buffersRDD = new CircleRDD(pointsRDD, distance)
      buffersRDD.analyze(envelope, nPointsRDD.toInt)
      buffersRDD.spatialPartitioning(pointsRDD.getPartitioner)
      buffersRDD.spatialPartitionedRDD.cache
      n(buffersRDD.spatialPartitionedRDD.count(), "Buffers")
      (pointsRDD, buffersRDD, usingIndex)
    }

    // Finding pairs...
    val considerBoundary = true
    val pairs = timer{header("B.Pairs found")}{
      val pairs = JoinQuery.DistanceJoinQueryFlat(points, buffers, usingIndex, considerBoundary)
        .rdd.map{ pair =>
          val id1 = pair._1.getUserData().toString().split("\t").head.trim().toInt
          val p1  = pair._1.getCentroid
          val id2 = pair._2.getUserData().toString().split("\t").head.trim().toInt
          val p2  = pair._2
          ( (id1, p1) , (id2, p2) )
        }.filter(p => p._1._1 < p._2._1).cache
      n(pairs.count(), "Pairs")
      pairs
    }

    // Finding centers...
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
    val (centers, nCenters) = timer{header("C.Centers found")}{
      val centersPairs = pairs.map{ p =>
        val p1 = p._1._2
        val p2 = p._2._2
        calculateCenterCoordinates(p1, p2, r2)
      }
      val centers = centersPairs.map(_._1).union(centersPairs.map(_._2)).cache
      val nCenters = centers.count()
      n(nCenters, "Centers")
      (centers, nCenters)
    }

    // Finding disks...
    val r = params.epsilon() / 2.0
    val disks = timer{header("D.Disks found")}{
      val centersRDD = new PointRDD(centers, StorageLevel.MEMORY_ONLY)
      val centersBuffer = new CircleRDD(centersRDD, r + params.precision())
      centersBuffer.analyze(envelope, nCenters.toInt)
      centersBuffer.spatialPartitioning(points.getPartitioner)
      centersBuffer.spatialPartitionedRDD.cache()
      val disks = JoinQuery.DistanceJoinQuery(points, centersBuffer, usingIndex, considerBoundary).cache()
      n(disks.count(), "Disks")
      disks
    }

    // Cleaning disks...
    val mu = params.mu()
    timer{header("Disks cleaned")}{
      val d = disks.rdd.map{ d =>
        val points = d._2.asScala.toArray
        val centroid = geofactory.createMultiPoint(points).getEnvelope().getCentroid
        val pids = points.map(_.getUserData.toString().split("\t").head.toInt).sorted.mkString(" ")
        centroid.setUserData(pids)
        centroid
      }.distinct().cache()
      n(d.count(), "Disks")
      d
    }

    logger.info("Closing session...")
    spark.close()
    logger.info("Closing session... Done!")
  }
}
