import org.slf4j.{LoggerFactory, Logger}
import org.rogach.scallop._
import scala.collection.JavaConverters._
import scala.collection.immutable.HashSet
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.TaskContext
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, CircleRDD, PointRDD}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geosparksql.utils.{Adapter, GeoSparkSQLRegistrator}
import org.datasyslab.geospark.utils.HalfOpenRectangle
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate, Point, Polygon}
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.index.SpatialIndex;
import edu.ucr.dblab.Utils._

object GeoTesterRDD_Viz{
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class PointWKT(wkt: String, id: Int, t: Int)

  case class ST_Point(id: Int, x: Double, y: Double, t: Int){
    def asWKT = PointWKT(s"POINT($x $y)", id, t)
  }

  def main(args: Array[String]): Unit = {
    logger.info("Starting session...")
    implicit val params = new DistanceJoinConf(args)
    val appName = s"GeoTesterRDD: " +
    s"epslion=${params.epsilon()} " +
    s"grid=${params.capacity()}"
    implicit val debugOn  = params.debug()
    implicit val geofactory = new GeometryFactory()
    implicit val spark = SparkSession.builder()
      .appName(appName)
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
    def getConf(property: String)(implicit conf: SparkConf): String = conf.get(property)
    val appId: String = if(getConf("spark.master").contains("local")){
      getConf("spark.app.id")
    } else {
      getConf("spark.app.id").takeRight(4)
    }
    def header(msg: String): String = s"GeoTesterRDD|${appId}|$msg|Time"
    def n(msg:String, count: Long): Unit = {
      logger.info(s"GeoTesterRDD|${appId}|$msg|Load|$count")
    }
    logger.info("Starting session... Done!")
    logger.info(s"Level of parallelism: ${getConf("spark.default.parallelism")}")

    val parallelism = params.parallelism()
    val (pointsRDD, nPointsRDD, envelope) = timer{"Reading data"}{
      val pointsSchema = ScalaReflection.schemaFor[ST_Point].dataType.asInstanceOf[StructType]
      val pointsRaw = spark.read.schema(pointsSchema)
        .option("delimiter", "\t").option("header", false)
        .csv(params.input()).as[ST_Point]
        .repartition(parallelism)
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
      val envelope = pointsRDD.boundaryEnvelope
      envelope.expandBy( (params.epsilon() / 2.0) + params.precision() )
      (pointsRDD, nPointsRDD, envelope)
    }

    val distance = params.epsilon() + params.precision()
    val stage = "Partitions done"
    val (points, buffers, npartitions) = timer{stage}{
      pointsRDD.spatialPartitioning(gridtype, params.partitions())
      pointsRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
      val npartitions = pointsRDD.getPartitioner.getGrids.size()
      val buffersRDD = new CircleRDD(pointsRDD, distance)
      buffersRDD.analyze(envelope, nPointsRDD.toInt)
      buffersRDD.spatialPartitioning(pointsRDD.getPartitioner)
      buffersRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
      n(stage, buffersRDD.spatialPartitionedRDD.count())
      (pointsRDD, buffersRDD, npartitions)
    }

    //
    debug{
      save("/tmp/edgesPoints.wkt"){
        pointsRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex({ case(index, iter) =>
          iter.map{ point =>
            s"${point.toText()}\t${index}\n"
          }}, preservesPartitioning = true)
          .collect().sorted
      }
    }

    logger.info(s"GridType: $gridtype.")
    implicit val grids = pointsRDD.partitionTree.getLeafZones.asScala.toVector
      .sortBy(_.partitionId).map(_.getEnvelope)

    //
    debug{
      save{"/tmp/edgesGGrids.wkt"}{
        pointsRDD.partitionTree.getLeafZones.asScala.map{ z =>
          val id = z.partitionId
          val e = z.getEnvelope
          val (x1,x2,y1,y2) = (e.getMinX, e.getMaxX, e.getMinY, e.getMaxY)
          val p1 = new Coordinate(x1, y1)
          val p2 = new Coordinate(x2, y1)
          val p3 = new Coordinate(x2, y2)
          val p4 = new Coordinate(x1, y2)
          val p = geofactory.createPolygon(Array(p1,p2,p3,p4,p1))
          s"${p.toText()}\t${id}\n"
        }
      }
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
    val stageA = "A.Pairs and centers found"
    val (centers, nCenters) = timer{header(stageA)}{
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
      n(stageA, nCenters)
      (centers, nCenters)
    }

    //
    debug{
      save("/tmp/edgesCenters.wkt"){
        val centersRDD = new PointRDD(centers, StorageLevel.MEMORY_ONLY)
        centersRDD.analyze(envelope, nCenters.toInt)
        centersRDD.spatialPartitioning(points.getPartitioner)
        centersRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex({ case(index, iter) =>
          iter.map{ point =>
            s"${point.toText()}\t${index}\n"
          }}, preservesPartitioning = true)
          .collect().sorted
      }
    }

    // Finding disks...
    val r = params.epsilon() / 2.0
    val stageB = "DJOIN|GeoSpark"
    val disks = timer{header(stageB)}{
      val centersRDD = new PointRDD(centers, StorageLevel.MEMORY_ONLY)
      centersRDD.analyze(envelope, nCenters.toInt)
      centersRDD.spatialPartitioning(points.getPartitioner)
      centersRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
      val pointsBuffer = new CircleRDD(points, r + params.precision())
      pointsBuffer.spatialPartitioning(centersRDD.getPartitioner)
      pointsBuffer.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
      val usingIndex = indextype match {
        case Some(index) => {
          centersRDD.buildIndex(index, true)
          centersRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
          logger.info(s"IndexType: ${index.name()}.")
          true
        }
        case None => {
          centersRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
          logger.info("IndexType: None.")
          false
        }
      }      
      val disks = JoinQuery.DistanceJoinQueryFlat(centersRDD, pointsBuffer, usingIndex, considerBoundary)
      .rdd.map{ d =>
        val point  = d._1
        val center = d._2
        (center, HashSet(point))
      }.reduceByKey( (pids1, pids2) => pids1 ++ pids2)
       
      n(stageB, disks.count())
      disks
    }

    //
    debug{
      logger.info(s"Number of partition on Join: ${disks.getNumPartitions}.")
      save("/tmp/edgesJoin.wkt"){
        disks.mapPartitionsWithIndex({ case(index, iter) =>
          iter.map{ case(center, points) =>
            val pids = points.map(_.getUserData.toString.split("\t")(0)).map(_.toInt).toList.sorted.mkString(" ")
            s"${center.toText()}\t${pids}\t${index}\n"
          }}, preservesPartitioning = true)
          .collect().sorted
      }
    }

    // Alternative finding disks...
    val mu = params.mu()
    val d = (params.epsilon() / 2.0) + params.precision()
    val stageIB = "DJOIN|Index based"
    val results = timer(header(stageIB)){
      val centersRDD = new PointRDD(centers, StorageLevel.MEMORY_ONLY)
      centersRDD.analyze(envelope, nCenters.toInt)
      centersRDD.spatialPartitioning(points.getPartitioner)
      centersRDD.buildIndex(IndexType.QUADTREE, true)
      centersRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)

      val bufferRDD = new PointRDD(pointsRDD.rawSpatialRDD.rdd, StorageLevel.MEMORY_ONLY)

      val results = DistanceJoin.join(centersRDD, bufferRDD, d)
      n(stageIB, results.count())

      results
    }

    //
    debug{
      save("/tmp/edgesMyJoin.wkt"){
        results.mapPartitionsWithIndex({ case(index, iter) =>
          iter.map{ case(center, points) =>
            val pids = points.map(_.getUserData.toString.split("\t")(0)).map(_.toInt).sorted.mkString(" ")
            s"${center.toText()}\t${pids}\t${index}\n"
          }}, preservesPartitioning = true)
          .collect().sorted
      }
    }
    
    // Partition based Quadtree ...
    val fraction = params.fraction()
    val levels   = params.levels()
    val capacity = params.capacity()

    val pointsRDD2 = new CircleRDD(new PointRDD(pointsRDD.rawSpatialRDD.rdd, StorageLevel.MEMORY_ONLY), d)
    pointsRDD2.analyze(envelope, nPointsRDD.toInt)
    pointsRDD2.spatialPartitioning(points.getPartitioner)
    pointsRDD2.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
    val centersRDD = new PointRDD(centers, StorageLevel.MEMORY_ONLY)
    centersRDD.analyze(envelope, nCenters.toInt)
    centersRDD.spatialPartitioning(pointsRDD2.getPartitioner)
    centersRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

    val stagePB1 = "DJOIN|Partition based 1"
    val partitionBased1 = timer(header(stagePB1)){
      val partitionBased = DistanceJoin.partitionBasedQuadtreeV1(centersRDD, pointsRDD2, d, capacity, fraction, levels)
      partitionBased.cache()
      n(stagePB1, partitionBased.count())
      partitionBased
    }

    //
    debug{
      logger.info(s"Capacity: ${capacity}")
      
      save("/tmp/edgesPPairs1.wkt"){
        partitionBased1.mapPartitionsWithIndex(
          {case(index, iter) =>
            iter.flatMap{ case(pairs, stats, lgrids, points) =>
              pairs.map{case(point, points) =>
                val pids = points.map(_.getUserData.toString().split("\t")(0))
                  .map(_.toInt).sorted.mkString(" ")
                s"${point.toText()}\t${pids}\t${index}\n"
                
              }
            }
          }, preservesPartitioning = true).collect().sorted
      }

      save(s"/tmp/edgesStats_${appId}.wkt"){
        partitionBased1.mapPartitionsWithIndex(
          {case(index, iter) =>
            iter.map{ case(pairs, stats, lgrids, points) => s"$stats\t$appId\n" }
          }, preservesPartitioning = true).collect().sorted
      }

      save(s"/tmp/edgesLGrids.wkt"){
        partitionBased1.mapPartitionsWithIndex(
          {case(index, iter) =>
            iter.map{ case(pairs, stats, lgrids, points) => s"$lgrids" }
          }, preservesPartitioning = true).collect()
      }

      save(s"/tmp/edgesQuads.wkt"){
        partitionBased1.mapPartitionsWithIndex(
          {case(index, iter) =>
            iter.map{ case(pairs, stats, lgrids, points) => s"$points" }
          }, preservesPartitioning = true).collect()
      }
      
    }

    val stagePB2 = "DJOIN|Partition based 2"
    val partitionBased2 = timer(header(stagePB2)){
      val partitionBased = DistanceJoin.partitionBasedQuadtreeV2(centersRDD, pointsRDD2, d, capacity, fraction, levels)
      partitionBased.cache()
      n(stagePB2, partitionBased.count())
      partitionBased
    }

    //
    debug{
      logger.info(s"Capacity: ${capacity}")
      
      save("/tmp/edgesPPairs2.wkt"){
        partitionBased2.mapPartitionsWithIndex(
          {case(index, iter) =>
            iter.flatMap{ case(pairs, stats, lgrids, points) =>
              pairs.map{case(point, points) =>
                val pids = points.map(_.getUserData.toString().split("\t")(0))
                  .map(_.toInt).sorted.mkString(" ")
                s"${point.toText()}\t${pids}\t${index}\n"
                
              }
            }
          }, preservesPartitioning = true).collect().sorted
      }

      save(s"/tmp/edgesStats2_${appId}.wkt"){
        partitionBased1.mapPartitionsWithIndex(
          {case(index, iter) =>
            iter.map{ case(pairs, stats, lgrids, points) => s"$stats\t$appId\n" }
          }, preservesPartitioning = true).collect().sorted
      }

      save(s"/tmp/edgesLGrids2.wkt"){
        partitionBased1.mapPartitionsWithIndex(
          {case(index, iter) =>
            iter.map{ case(pairs, stats, lgrids, points) => s"$lgrids" }
          }, preservesPartitioning = true).collect()
      }

      save(s"/tmp/edgesQuads2.wkt"){
        partitionBased1.mapPartitionsWithIndex(
          {case(index, iter) =>
            iter.map{ case(pairs, stats, lgrids, points) => s"$points" }
          }, preservesPartitioning = true).collect()
      }
      
    }

    /*
    // Partition based Grid ...
    val width = params.width()
    val partitionBased = timer("Partition based join"){
      val centersRDD = new PointRDD(centers, StorageLevel.MEMORY_ONLY)
      centersRDD.analyze(envelope, nCenters.toInt)
      centersRDD.spatialPartitioning(points.getPartitioner)
      val buffersRDD = new PointRDD(pointsRDD.rawSpatialRDD.rdd, StorageLevel.MEMORY_ONLY)

      val partitionBased = DistanceJoin.partitionBasedGrid(centersRDD, buffersRDD, d, width)
      n("Partition Based", partitionBased.count())
      partitionBased
    }.persist(StorageLevel.MEMORY_ONLY)
    logger.info(s"Grid width: ${width}")
    save("/tmp/edgesPPairs.wkt"){
      partitionBased.mapPartitionsWithIndex(
        {case(index, iter) =>
          iter.map{ case(center, points) =>
            val pids = points.map(_.getUserData.toString().split("\t")(0))
              .map(_.toInt).sorted.mkString(" ")
            s"${center.toText()}\t${pids}\t${index}\n"
          }
        }
          , preservesPartitioning = true).collect().sorted
    }
     */

    logger.info("Closing session...")
    logger.info(s"Number of partition on default quadtree: $npartitions.")
    logger.info(s"${appId}|${System.getProperty("sun.java.command")} --npartitions $npartitions")
    spark.close()
    logger.info("Closing session... Done!")
  }
}

class DistanceJoinConf(args: Seq[String]) extends ScallopConf(args) {
  val input = opt[String](default = Some(""))
  val epsilon = opt[Double](default = Some(10.0))
  val mu = opt[Int](default = Some(2))
  val precision = opt[Double](default = Some(0.001))
  val fraction = opt[Double](default = Some(0.01))
  val levels = opt[Int](default = Some(5))
  val capacity = opt[Int](default = Some(20))
  val width = opt[Double](default = Some(10.0))
  val partitions = opt[Int](default = Some(256))
  val parallelism = opt[Int](default = Some(324))
  val gridtype = opt[String](default = Some("quadtree"))
  val indextype = opt[String](default = Some("none"))
  val debug = opt[Boolean](default = Some(false))

  verify()
}
