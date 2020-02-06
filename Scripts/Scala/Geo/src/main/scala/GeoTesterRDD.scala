package edu.ucr.dblab

import org.slf4j.{LoggerFactory, Logger}
import org.rogach.scallop._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, CircleRDD}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.enums.{GridType, IndexType}
import com.vividsolutions.jts.geom.{Coordinate, Point}
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
      .appName("GeoTesterRDD")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    import spark.implicits._
    val indextype = params.indextype() match {
      case "rtree"    => IndexType.RTREE
      case "quadtree" => IndexType.QUADTREE
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

    val pointsRaw = timer{"Reading data"}{
      val pointsSchema = ScalaReflection.schemaFor[ST_Point].dataType.asInstanceOf[StructType]
      val points = spark.read.schema(pointsSchema)
        .option("delimiter", "\t").option("header", false)
        .csv(params.input()).as[ST_Point].rdd.cache
      n(points.count(), "Raw points")
      points
    }

    val points = timer{header("Casting geometries")}{
      val pointsRDD = new SpatialRDD[Point]
      val points = pointsRaw.map{ point =>
        val userData = s"${point.id}\t${point.t}"
        val p = geofactory.createPoint(new Coordinate(point.x, point.y))
        p.setUserData(userData)
        p
      }
      pointsRDD.setRawSpatialRDD(points)
      pointsRDD.rawSpatialRDD.cache()
      pointsRDD.analyze()
      pointsRDD
    }

    val distance = params.epsilon() + params.precision()    
    val pairs = timer{header("Distance self-join")}{
      points.spatialPartitioning(gridtype, params.partitions())
      points.spatialPartitionedRDD.cache()
      val pointsBuffer = new CircleRDD(points, distance)
      pointsBuffer.analyze()
      pointsBuffer.spatialPartitioning(points.getPartitioner)
      points.buildIndex(indextype, true)

      val considerBoundary = true
      val usingIndex = true
      val pairs = JoinQuery.DistanceJoinQueryFlat(points, pointsBuffer, usingIndex, considerBoundary)
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

    logger.info("Closing session...")
    spark.close()
    logger.info("Closing session... Done!")
  }
}
