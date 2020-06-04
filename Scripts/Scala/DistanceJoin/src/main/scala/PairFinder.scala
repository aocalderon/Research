package edu.ucr.dblab.djoin

import org.slf4j.{LoggerFactory, Logger}
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

object PairsFinderTest{
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class ST_Point(id: Int, x: Double, y: Double, t: Int){
    def asWKT: String = s"POINT($x $y)\t$id\t$t\n"
  }

  def main(args: Array[String]): Unit = {
    logger.info("Starting session...")
    implicit val params = new PairsFinderConf(args)
    val appName = s"PairFinderTest"
    implicit val debugOn  = params.debug()
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
    val global_grids = pointsRDD.partitionTree.getLeafZones.asScala.toVector
      .sortBy(_.partitionId).map(_.getEnvelope)
    implicit val settings = Settings(spark, logger, global_grids, debugOn)
    val npartitions = global_grids.size
    val distance = (params.epsilon() / 2.0) + params.precision()

    // Joining points...
    val considerBoundary = true
    val usingIndex = false
    val epsilon = params.epsilon() + params.precision()
    val method = params.method()
    val capacity = params.capacity()
    val joinStage = "Join done"

    val (joinRDD, nJoinRDD) = timer{ header(joinStage) }{
      val leftRDD = pointsRDD
      val rightRDD = new CircleRDD(pointsRDD, epsilon)
      rightRDD.analyze()
      rightRDD.spatialPartitioning(leftRDD.getPartitioner)

      val join = method match {
        case "Geospark" => { // GeoSpark distance join...
          DistanceJoin.join(leftRDD, rightRDD)
        }
        case "Baseline" => { // Baseline distance join...
          DistanceJoin.baseline(leftRDD, rightRDD)
        }
        case "Index" => { // Index based Quadtree ...
          DistanceJoin.indexBased(leftRDD, rightRDD)
        }
        case "Partition" => { // Partition based Quadtree ...
          DistanceJoin.partitionBasedByQuadtree(leftRDD, rightRDD, capacity)
        }
      }
      join.cache()
      val nJoin = join.count()
      n(joinStage, nJoin)
      (join, nJoin)
    }

    // Finding pairs...
    val pairsStage = "Pairs found"
    val (pairsRDD, nPairsRDD) = timer{ header(pairsStage) }{
      val pairs = joinRDD.map{ pair =>
        val id1 = pair._1.getUserData().toString().split("\t").head.trim().toInt
        val p1  = pair._1.getCentroid
        val id2 = pair._2.getUserData().toString().split("\t").head.trim().toInt
        val p2  = pair._2
        ( (id1, p1) , (id2, p2) )
      }.filter(p => p._1._1 < p._2._1).map(p => (p._1._2, p._2._2))
      pairs.cache()
      val nPairs = pairs.count()
      n(pairsStage, nPairs)
      (pairs, nPairs)
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
      def saveJoin(rdd: RDD[(Point, Point)], filename: String): Unit = {
        save{filename}{
          rdd.map{ case(source, target) =>
            val coords = Array(new Coordinate(source.getX, source.getY),
              new Coordinate(target.getX, target.getY))
            val line = geofactory.createLineString(coords)
            s"${line.toText}\t${source.getUserData}\t${target.getUserData}\n"
          }.collect.sorted
        }
      }

      saveJoin(pairsRDD, s"/tmp/edges${params.method()}.wkt")
    }
    
    logger.info("Closing session...")
    logger.info(s"Number of partition on default quadtree: $npartitions.")
    logger.info(s"${appId}|${System.getProperty("sun.java.command")} --npartitions $npartitions")
    spark.close()
    logger.info("Closing session... Done!")
  }
}

