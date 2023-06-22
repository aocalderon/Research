package edu.ucr.dblab.djoin

import org.slf4j.{LoggerFactory, Logger}
import scala.collection.JavaConverters._
import org.apache.spark.TaskContext
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
import edu.ucr.dblab.djoin.DistanceJoin.{geofactory, round, circle2point, envelope2polygon}

object FlockFinder{
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class ST_Point(id: Int, x: Double, y: Double, t: Int){
    def asWKT: String = s"POINT($x $y)\t$id\t$t\n"
  }

  def main(args: Array[String]): Unit = {
    logger.info("Starting session...")
    implicit val params = new FlockFinderConf(args)
    val appName = s"DiskFinder"
    implicit val debugOn = params.debug()
    implicit val spark = SparkSession.builder()
      .appName(appName)
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    GeoSparkSQLRegistrator.registerAll(spark)
    import spark.implicits._
    implicit val conf = spark.sparkContext.getConf
    def getConf(property: String)(implicit conf: SparkConf): String = conf.get(property)
    val appId: String = if(getConf("spark.master").contains("local")){
      getConf("spark.app.id")
    } else {
      getConf("spark.app.id").takeRight(4)
    }
    def header(msg: String): String = s"$appName|$appId|$msg|Time"
    def n(msg:String, count: Long): Unit = logger.info(s"$appName|$appId|$msg|Load|$count")
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

    val partitionStage = "Partitions done"
    val pointsRDD = timer{partitionStage}{
      pointsRaw.spatialPartitioning(GridType.QUADTREE, params.partitions())
      pointsRaw.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
      n(partitionStage, pointsRaw.spatialPartitionedRDD.count())
      pointsRaw
    }
    // Collecting the global settings...
    val epsilon = params.epsilon()
    val global_grids = pointsRDD.partitionTree.getLeafZones.asScala.toVector
      .sortBy(_.partitionId)
      .map{ partition =>
        val grid = partition.getEnvelope
        grid.expandBy(epsilon)
        grid
      }
    implicit val settings = Settings(spark, logger, global_grids, debugOn)
    val grids = pointsRDD.partitionTree.getLeafZones.asScala.toVector
      .sortBy(_.partitionId).map(_.getEnvelope)
    val broadcastGrids = spark.sparkContext.broadcast(grids)

    // Joining points...
    val considerBoundary = true
    val usingIndex = false
    val r = epsilon / 2.0
    val r2 = math.pow(r, 2)
    val method = params.method()
    val capacity = params.capacity()
    val stage = "Disk stage"

    val (resultsRDD, nResultsRDD) = timer{ header(stage) }{
      //val leftRDD = pointsRDD
      val circlesRDD = new CircleRDD(pointsRDD, epsilon)
      circlesRDD.analyze()
      circlesRDD.spatialPartitioning(pointsRDD.getPartitioner)

      val results = method match {
        case "Partition" => { // Partition based Quadtree ...
          val points = circlesRDD.spatialPartitionedRDD.rdd.map(circle2point)
          
          points.zipPartitions(points, preservesPartitioning = true){ (leftIt, rightIt) =>
            val global_gid = TaskContext.getPartitionId

            // Finding pairs...
            val pairs = DistanceJoin.partitionBasedIterator(leftIt, rightIt, epsilon)
              .filter{ case(l,r) =>
                val i = l.getUserData.toString.split("\t")(0)
                val j = r.getUserData.toString.split("\t")(0)
                i < j
              }
              .toVector

            val grid = broadcastGrids.value(global_gid)
            val centers = pairs.flatMap{ case (p1, p2) =>
              calculateCenterCoordinates(p1, p2, r2)
            }.filter(center => grid.contains(center.getCoordinate))
            
            val points = pairs.flatMap{ case(p1, p2) => List(p1, p2)}.distinct
            val disks = DistanceJoin
              .partitionBasedAggregate(centers.toIterator, points.toIterator, r)
              .toVector

            disks.toIterator
          }
        }
      }
      results.cache()
      val N = results.count()
      n(stage, N)
      (results, N)
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
      save{"/tmp/edgesGGrids.wkt"}{
        pointsRDD.partitionTree.getLeafZones.asScala.map{ z =>
          val id = z.partitionId
          val e = z.getEnvelope
          val p = DistanceJoin.envelope2polygon(e)
          s"${p.toText()}\t${id}\n"
        }
      }
    }

    //
    debug{
      /*
      save{"/tmp/edgesPairs.wkt"}{
        pairsRDD.map{ case(source, target) =>
          val coords = Array(source.getCoordinate, target.getCoordinate)
          val line = geofactory.createLineString(coords)
          s"${line.toText}\n"
        }.collect.sorted
      }
      save{"/tmp/edgesCenters.wkt"}{
        centersRDD.map{ center =>
          s"${center.toText}\n"
        }.collect.sorted
      }
       */
      save{"/tmp/edgesDisks.wkt"}{
        resultsRDD.map{ disk =>
          val center = disk._1
          val circle = center.buffer(epsilon / 2.0, 10)
          val pids = disk._2.map(_.getUserData.toString.split("\t")(0)).mkString(" ")  
          s"${circle.toText}\t${pids}\n"
        }.collect.sorted
      }
      
    }
    
    logger.info("Closing session...")
    logger.info(s"${appId}|${System.getProperty("sun.java.command")} --npartitions ${global_grids.size}")
    spark.close()
    debug{
      save{"/tmp/edgesLGrids.wkt"}{
        (0 until global_grids.size).flatMap{ n =>
          try{
            scala.io.Source
              .fromFile(s"/tmp/edgesLGrids_${n}.wkt")
              .getLines.map{ wkt =>
                s"$wkt\n"
              }
          } catch {
            case e: Exception => None
          }
        }
      }
    }
    logger.info("Closing session... Done!")
  }

  def calculateCenterCoordinates(p1: Point, p2: Point, r2: Double,
    delta: Double = 0.001): List[Point] = {

    val X: Double = p1.getX - p2.getX
    val Y: Double = p1.getY - p2.getY
    val D2: Double = math.pow(X, 2) + math.pow(Y, 2)
    if (D2 != 0.0){
      val root: Double = math.sqrt(math.abs(4.0 * (r2 / D2) - 1.0))
      val h1: Double = ((X + Y * root) / 2) + p2.getX
      val k1: Double = ((Y - X * root) / 2) + p2.getY
      val h2: Double = ((X - Y * root) / 2) + p2.getX
      val k2: Double = ((Y + X * root) / 2) + p2.getY
      val h = geofactory.createPoint(new Coordinate(h1,k1))
      val k = geofactory.createPoint(new Coordinate(h2,k2))
      List(h, k)
    } else {
      val p2_prime = geofactory.createPoint(new Coordinate(p2.getX + delta, p2.getY))
      calculateCenterCoordinates(p1, p2_prime, r2)
    }
  }  
}

