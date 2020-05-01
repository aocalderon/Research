package edu.ucr.dblab.djoin

import org.slf4j.{LoggerFactory, Logger}
import org.rogach.scallop._
import scala.collection.JavaConverters._
import scala.util.Random
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, CircleRDD, PointRDD}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.geometryObjects.Circle
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate, Point, Polygon, MultiPolygon}
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.index.SpatialIndex
import com.vividsolutions.jts.index.quadtree._
import edu.ucr.dblab.Utils._
import edu.ucr.dblab.{StandardQuadTree, QuadRectangle}

object DistanceJoinTest{
  implicit val geofactory = new GeometryFactory()
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class ST_Point(id: Int, x: Double, y: Double, t: Int){
    def asWKT: String = s"POINT($x $y)\t$id\t$t\n"
  }
  case class ST_Center(x: Double, y: Double){
    def asWKT: String = s"POINT($x $y)\n"
  }

  def main(args: Array[String]): Unit = {
    logger.info("Starting session...")
    implicit val params = new DistanceJoinTestConf(args)
    val appName = s"DistanceJoinTest"
    implicit val debugOn  = params.debug()
    implicit val spark = SparkSession.builder()
      .appName(appName)
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    GeoSparkSQLRegistrator.registerAll(spark)
    import spark.implicits._
    def header(msg: String): String = s"GeoTesterRDD|$msg|Time"
    def n(msg:String, count: Long): Unit = {
      logger.info(s"GeoTesterRDD|$msg|Load|$count")
    }
    implicit val conf = spark.sparkContext.getConf
    def getConf(property: String)(implicit conf: SparkConf): String = conf.get(property)
    val appId: String = if(getConf("spark.master").contains("local")){
      getConf("spark.app.id")
    } else {
      getConf("spark.app.id").takeRight(4)
    }
    logger.info("Starting session... Done!")

    val (pointsRDD, nPointsRDD) = timer{"Reading points"}{
      val pointsSchema = ScalaReflection.schemaFor[ST_Point].dataType.asInstanceOf[StructType]
      val pointsRaw = spark.read.schema(pointsSchema)
        .option("delimiter", "\t").option("header", false)
        .csv(params.points()).as[ST_Point]
        .rdd
      val pointsRDD = new SpatialRDD[Point]
      val points = pointsRaw.map{ point =>
        val userData = s"${point.id}\t${point.t}"
        val p = geofactory.createPoint(new Coordinate(point.x, point.y))
        p.setUserData(userData)
        p
      }
      pointsRDD.setRawSpatialRDD(points)
      pointsRDD.analyze()
      pointsRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY)
      val nPointsRDD = pointsRDD.rawSpatialRDD.count()
      n("Data", nPointsRDD)
      (pointsRDD, nPointsRDD)
    }

    val (centersRDD, nCentersRDD) = timer{"Reading centers"}{
      val centersSchema = ScalaReflection.schemaFor[ST_Center].dataType.asInstanceOf[StructType]
      val centersRaw = spark.read.schema(centersSchema)
        .option("delimiter", "\t").option("header", false)
        .csv(params.centers()).as[ST_Center]
        .rdd
      val centersRDD = new SpatialRDD[Point]
      val centers = centersRaw.map{ center =>
        geofactory.createPoint(new Coordinate(center.x, center.y))
      }
      centersRDD.setRawSpatialRDD(centers)
      centersRDD.analyze()
      centersRDD.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY)
      val nCentersRDD = centersRDD.rawSpatialRDD.count()
      n("Data", nCentersRDD)
      (centersRDD, nCentersRDD)
    }

    val stage = "Partitions done"
    val npartitions = timer{stage}{
      pointsRDD.spatialPartitioning(GridType.QUADTREE, params.partitions())
      pointsRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
      val npartitions = pointsRDD.getPartitioner.getGrids.size()
      n(stage, pointsRDD.spatialPartitionedRDD.count())
      npartitions
    }
    implicit val grids = pointsRDD.partitionTree.getLeafZones.asScala.toVector
      .sortBy(_.partitionId).map(_.getEnvelope)

    //
    debug{
      save("/tmp/edgesPoints.wkt"){
        pointsRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex({ case(index, iter) =>
          iter.map{ point =>
            s"${point.toText()}\t${point.getUserData.toString}\t${index}\n"
          }}, preservesPartitioning = true)
          .collect().sorted
      }
      save("/tmp/edgesCenters.wkt"){
        //centersRDD.spatialPartitioning(pointsRDD.getPartitioner)
        centersRDD.rawSpatialRDD.rdd.mapPartitionsWithIndex({ case(index, iter) =>
          iter.map{ point =>
            s"${point.toText()}\t${index}\n"
          }}, preservesPartitioning = true)
          .collect().sorted
      }
      save{"/tmp/edgesGGrids.wkt"}{
        pointsRDD.partitionTree.getLeafZones.asScala.map{ z =>
          val id = z.partitionId
          val e = z.getEnvelope
          val p = DistanceJoinV2.envelope2polygon(e)
          s"${p.toText()}\t${id}\n"
        }
      }
    }

    // GeoSpark distance join...
    val distance = params.epsilon() + params.precision()
    val stageB = "DJOIN|GeoSpark"
    val geospark = timer{header(stageB)}{
      val circlesRDD = new CircleRDD(centersRDD, distance / 2.0)
      circlesRDD.spatialPartitioning(pointsRDD.getPartitioner)
      val geospark = DistanceJoinV2.join(pointsRDD, circlesRDD)
      geospark.cache()
      n(stageB, geospark.count())
      geospark
    }
    
    // Partition based Quadtree ...
    val fraction = params.fraction()
    val levels   = params.levels()
    val capacity = params.capacity()
    
    val stagePB1 = "DJOIN|Partition based"
    val partitionBased1 = timer(header(stagePB1)){
      val circlesRDD = new CircleRDD(centersRDD, distance / 2.0)
      circlesRDD.spatialPartitioning(pointsRDD.getPartitioner)
      val partitionBased = DistanceJoinV2.partitionBasedQuadtreeV2(pointsRDD, circlesRDD, capacity, fraction, levels)
      partitionBased.cache()
      n(stagePB1, partitionBased.count())
      partitionBased
    }

    //
    debug{
      save("/tmp/edgesGJoin.wkt"){
        geospark.mapPartitionsWithIndex(
          { case(index, iter) =>
            iter.map{ case(point, points) =>
              val pointWKT  = s"${point.toText()}\t${point.getUserData.toString()}"
              val pointsWKT = points.map{ p =>
                s"${p.getUserData.toString().split("\t")(0)}"
              }.map(_.toInt).toList.sorted.mkString(" ")
              s"$pointWKT\t$index\t${points.size}\t${pointsWKT}\n"
            }
          }, preservesPartitioning = true).collect().sorted
      }
    
      save("/tmp/edgesPBJoin.wkt"){
        partitionBased1.mapPartitionsWithIndex(
          { case(index, iter) =>
            iter.map{ case(point, points) =>
              val pointWKT  = s"${point.toText()}\t${point.getUserData.toString()}"
              val pointsWKT = points.map{ p =>
                s"${p.getUserData.toString().split("\t")(0)}"
              }.map(_.toInt).toList.sorted.mkString(" ")
              s"$pointWKT\t$index\t${points.size}\t${pointsWKT}\n"
            }
          }, preservesPartitioning = true).collect().sorted
      }
    }
    
    logger.info("Closing session...")
    logger.info(s"Number of partition on default quadtree: $npartitions.")
    logger.info(s"${appId}|${System.getProperty("sun.java.command")} --npartitions $npartitions")
    spark.close()
    logger.info("Closing session... Done!")
  }
}

class DistanceJoinTestConf(args: Seq[String]) extends ScallopConf(args) {
  val points     = opt[String](default = Some(""))
  val centers    = opt[String](default = Some(""))
  val epsilon    = opt[Double](default = Some(10.0))
  val mu         = opt[Int](default = Some(2))
  val precision  = opt[Double](default = Some(0.001))
  val capacity   = opt[Int](default = Some(20))
  val fraction   = opt[Double](default = Some(0.01))
  val levels     = opt[Int](default = Some(5))
  val partitions = opt[Int](default = Some(256))
  val debug      = opt[Boolean](default = Some(false))

  verify()
}
