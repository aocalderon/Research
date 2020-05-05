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
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate, Point, Polygon, MultiPolygon}
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.index.SpatialIndex
import com.vividsolutions.jts.index.quadtree._
import edu.ucr.dblab.Utils._
import edu.ucr.dblab.djoin.DistanceJoin.{geofactory, round, circle2point}

object DiskFinderTest{
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

    val (pointsRaw, nPoints) = timer{"Reading points"}{
      val pointsSchema = ScalaReflection.schemaFor[ST_Point].dataType.asInstanceOf[StructType]
      val pointsInput = spark.read.schema(pointsSchema)
        .option("delimiter", "\t").option("header", false)
        .csv(params.points()).as[ST_Point]
        .rdd
      val pointsRaw = new SpatialRDD[Point]
      val pointsJTS = pointsInput.map{ point =>
        val userData = s"${point.id}\t${point.t}"
        val p = geofactory.createPoint(new Coordinate(point.x, point.y))
        p.setUserData(userData)
        p
      }
      pointsRaw.setRawSpatialRDD(pointsJTS)
      pointsRaw.analyze()
      pointsRaw.rawSpatialRDD.persist(StorageLevel.MEMORY_ONLY)
      val nPointsRaw = pointsRaw.rawSpatialRDD.count()
      n("Points", nPointsRaw)
      (pointsRaw, nPointsRaw)
    }

    val stage = "Partitions done"
    val pointsRDD = timer{stage}{
      pointsRaw.spatialPartitioning(GridType.QUADTREE, params.partitions())
      pointsRaw.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
      n(stage, pointsRaw.spatialPartitionedRDD.count())
      pointsRaw
    }
    implicit val grids = pointsRDD.partitionTree.getLeafZones.asScala.toVector
      .sortBy(_.partitionId).map(_.getEnvelope)
    val npartitions = grids.size
    val distance = (params.epsilon() / 2.0) + params.precision()

    // Finding pairs and centers...
    val r2: Double = math.pow(params.epsilon() / 2.0, 2)
    val considerBoundary = true
    val usingIndex = false
    val stageA = "Pairs and centers found"

    val (centersRDD, nCenters) = timer{header(stageA)}{
      val epsilon = params.epsilon() + params.precision()
      val buffersRDD = new CircleRDD(pointsRDD, epsilon)
      buffersRDD.analyze()
      buffersRDD.spatialPartitioning(pointsRDD.getPartitioner)
      val pairs = JoinQuery.DistanceJoinQueryFlat(pointsRDD, buffersRDD, usingIndex, considerBoundary)
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
      val centersJTS = pairs.map(_._1)
        .union(pairs.map(_._2))
        .persist(StorageLevel.MEMORY_ONLY)

      val centersRaw = new SpatialRDD[Point]()
      centersRaw.setRawSpatialRDD(centersJTS)
      centersRaw.analyze()
      centersRaw.spatialPartitioning(pointsRDD.getPartitioner)
      centersRaw.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
      val nCenters = centersRaw.spatialPartitionedRDD.count
      n(stageA, nCenters)
      (centersRaw, nCenters)
    }
    

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
        centersRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex({ case(index, iter) =>
          iter.map{ center =>
            s"${center.toText()}\t${center.getUserData.toString()}\t${index}\n"
          }}, preservesPartitioning = true)
          .collect().sorted
      }
      save{"/tmp/edgesGGrids.wkt"}{
        pointsRDD.partitionTree.getLeafZones.asScala.map{ z =>
          val id = z.partitionId
          val e = z.getEnvelope
          val p = DistanceJoin.envelope2polygon(e)
          s"${p.toText()}\t${id}\n"
        }
      }
    }

    val leftRDD = centersRDD
    val rightRDD = new CircleRDD(pointsRDD, distance)
    rightRDD.analyze()
    rightRDD.spatialPartitioning(leftRDD.getPartitioner)

    val pairs = params.method() match {
      case "None" => { // GeoSpark distance join...
        val stageB = "DJOIN|GeoSpark"
        timer{header(stageB)}{
          val geospark = DistanceJoin.join(leftRDD, rightRDD)
          geospark.cache()
          n(stageB, geospark.count())
          geospark
        }
      }
      case "Index" => { // Index based Quadtree ...
        val stageIB = "DJOIN|Index based"
        timer(header(stageIB)){
          val indexBased = DistanceJoin.indexBased(leftRDD, rightRDD)
          indexBased.cache()
          n(stageIB, indexBased.count())
          indexBased
        }
      }
      case "Partition" => { // Partition based Quadtree ...
        val fraction = params.fraction()
        val levels   = params.levels()
        val capacity = params.capacity()
        val stagePB = "DJOIN|Partition based"
        timer(header(stagePB)){
          val partitionBased = DistanceJoin.partitionBased(leftRDD, rightRDD, distance)
          partitionBased.cache()
          n(stagePB, partitionBased.count())
          partitionBased
        }
      }
    }

    //
    debug{
      def saveJoin(rdd: RDD[(Point, Point)], filename: String): Unit = {
        val rdd2 = rdd.map{ case(l, r) =>
          val l2 = geofactory.createPoint(new Coordinate(round(l.getX), round(l.getY)))
          l2.setUserData(l.getUserData)

          val r2 = geofactory.createPoint(new Coordinate(round(r.getX), round(r.getY)))
          r2.setUserData(r.getUserData)

          (l2, r2)
        }

        save{filename}{
          val toSave = timer{"Grouping"}{
            val toSave =  DistanceJoin.groupByRightPoint(rdd)
            toSave.cache()
            n("Grouping", toSave.count())
            toSave
          }
         toSave.map{ case(point, points) =>
            val pointWKT  = s"${point.toText}\t${point.getUserData.toString}"
            val pids = points.map(_.getUserData.toString.split("\t").head.toInt)
              .toList.sorted.mkString(" ")

            s"$pointWKT\t$pids\n"
          }.collect.sorted
        }
      }

      saveJoin(pairs, s"/tmp/edges${params.method()}.wkt")
    }
    
    logger.info("Closing session...")
    logger.info(s"Number of partition on default quadtree: $npartitions.")
    logger.info(s"${appId}|${System.getProperty("sun.java.command")} --npartitions $npartitions")
    spark.close()
    logger.info("Closing session... Done!")
  }

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
}

class DiskFinderTestConf(args: Seq[String]) extends ScallopConf(args) {
  val points     = opt[String](default = Some(""))
  val epsilon    = opt[Double](default = Some(10.0))
  val mu         = opt[Int](default = Some(2))
  val precision  = opt[Double](default = Some(0.001))
  val capacity   = opt[Int](default = Some(20))
  val fraction   = opt[Double](default = Some(0.01))
  val levels     = opt[Int](default = Some(5))
  val partitions = opt[Int](default = Some(256))
  val method     = opt[String](default = Some("None"))
  val debug      = opt[Boolean](default = Some(false))

  verify()
}
