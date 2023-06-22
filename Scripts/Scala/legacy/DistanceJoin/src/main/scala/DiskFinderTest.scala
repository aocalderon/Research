package edu.ucr.dblab.djoin

import org.slf4j.{LoggerFactory, Logger}
import scala.collection.JavaConverters._
import org.apache.spark.TaskContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.apache.spark.sql.functions
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
import edu.ucr.dblab.djoin.SPMF.{AlgoLCM2, Transactions, Transaction}
import ch.cern.sparkmeasure.TaskMetrics

object DisksFinderTest{
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class ST_Point(id: Int, x: Double, y: Double, t: Int){
    def asWKT: String = s"POINT($x $y)\t$id\t$t\n"
  }

  def main(args: Array[String]): Unit = {
    logger.info("Starting session...")
    implicit val params = new DiskFinderConf(args)
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
    def getPhaseMetrics(metrics: TaskMetrics, phaseName: String): Dataset[Row] = {
      metrics.createTaskMetricsDF("PhaseX")
        .withColumn("appId", functions.lit(appId))
        .withColumn("phaseName", functions.lit(phaseName))
        .withColumn("epsilon", functions.lit(params.epsilon()))
        .orderBy("launchTime")
    }
    val persistanceLevel = params.persistance() match {
      case 0 => StorageLevel.NONE
      case 1 => StorageLevel.MEMORY_ONLY
      case 2 => StorageLevel.MEMORY_ONLY_SER
    }
    val metrics = TaskMetrics(spark)
    val epsilon = params.epsilon() + params.precision()
    val r = (epsilon / 2.0) 
    val r2 = math.pow(r, 2)
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
      }.repartition(1024)
      pointsRaw.setRawSpatialRDD(pointsJTS)
      val nPointsRaw = pointsRaw.rawSpatialRDD.count()
      val grid = pointsRaw.boundary()
      grid.expandBy(epsilon)
      pointsRaw.analyze(grid, nPointsRaw.toInt)
      pointsRaw.rawSpatialRDD.persist(persistanceLevel)
      n("Points", nPointsRaw)
      (pointsRaw, nPointsRaw)
    }

    val partitionStage = "Partitions done"
    val (pointsRDD, nPartitionRaw) = timer{partitionStage}{
      pointsRaw.spatialPartitioning(GridType.QUADTREE, params.partitions())
      pointsRaw.spatialPartitionedRDD.persist(persistanceLevel)
      val N = pointsRaw.spatialPartitionedRDD.count()
      n(partitionStage, N)
      (pointsRaw, N)
    }
    pointsRDD.spatialPartitionedRDD.rdd.persist(StorageLevel.MEMORY_ONLY)
    pointsRDD.spatialPartitionedRDD.rdd.count()
    // Collecting the global settings...
    val quadtree = pointsRDD.partitionTree
    val expanded_grids = quadtree.getLeafZones.asScala.toVector
      .sortBy(_.partitionId)
      .map{ partition =>
        val grid = partition.getEnvelope
        grid.expandBy(2 * epsilon)
        grid
      }
    implicit val settings = Settings(spark, logger, expanded_grids, debugOn)
    val grids = quadtree.getLeafZones.asScala.toVector
      .sortBy(_.partitionId).map(_.getEnvelope)
    val broadcastGrids = spark.sparkContext.broadcast(grids)
    val broadcastExpandedGrids = spark.sparkContext.broadcast(expanded_grids)

    logger.info("Printing partition location:")
    import java.net.{NetworkInterface, InetAddress}
    val P = pointsRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ (index, it) =>
      val hostNames = scala.collection.mutable.ListBuffer[(Int, String)]()
      
      val e = NetworkInterface.getNetworkInterfaces
      while(e.hasMoreElements)
      {
        val n = e.nextElement match {
          case e: NetworkInterface => e
          case _ => ???
        }
        val ee = n.getInetAddresses
        while (ee.hasMoreElements) {
          val h = ee.nextElement match {
            case e: InetAddress => {
                (index, e.getHostName)
            } 
            case _ => ???
          }
          hostNames += h
        }
      }
      
      hostNames.toIterator
    }.filter(_._2 != "localhost").toDF("pid", "host")
      .groupBy("host").agg(functions.count("pid").alias("n"))
    logger.info(s"${P.count}")
    P.show()
    logger.info("Printing partition location: Done!")

    // Joining points...
    val considerBoundary = true
    val usingIndex = false
    val method = params.method()
    val capacity = params.capacity()
    val disksPhase = "Disks phase"

    metrics.begin()
    val (candidatesRaw, nCandidatesRaw) = timer{ header(disksPhase) }{
      val circlesRDD = new CircleRDD(pointsRDD, 2 * epsilon)
      circlesRDD.analyze()
      circlesRDD.spatialPartitioning(pointsRDD.getPartitioner)

      val results = method match {
        case "Partition" => { // Partition based Quadtree ...
          val points = circlesRDD.spatialPartitionedRDD.rdd.map(circle2point)
          
          points.zipPartitions(points, preservesPartitioning = true){ (leftIt, rightIt) =>
            val tc = TaskContext.get
            val global_gid = tc.partitionId
            val taskId = tc.taskAttemptId()
            logger.info(s"PartitionId=${global_gid}\tTaskId=${taskId}")

            // Finding pairs...
            val points = leftIt.toVector
            val pairs = DistanceJoin.partitionBasedIterator(points.toIterator, rightIt, epsilon)
              .filter{ case(l,r) =>
                val i = l.getUserData.toString.split("\t")(0)
                val j = r.getUserData.toString.split("\t")(0)
                i < j
              }
              .toVector

            // Finding centers...
            val grid = broadcastExpandedGrids.value(global_gid)
            val centers = pairs.flatMap{ case (p1, p2) =>
              calculateCenterCoordinates(p1, p2, r2)
            }

            // Finding disks...
            val candidates = DistanceJoin
              .partitionBasedAggregate(centers.toIterator, points.toIterator, r)
              .toVector

            // Pruning duplicates/redundants...
            val transactions = candidates.map{ disk =>
              val pids = disk._2.map(_.getUserData.toString.split("\t").head.toInt)
                .toList.sorted.mkString(" ")
              val x = disk._1.getX
              val y = disk._1.getY

              (pids, (x, y))
            }.groupBy(_._1).values.map{ p =>
              val (pids, coords) = p.head
              val x = coords._1
              val y = coords._2

              new Transaction(x, y, pids)
            }.toList.distinct
            val data = new Transactions(transactions.asJava, 0)
            val lcm = new AlgoLCM2()
            lcm.run(data)
            grid.expandBy(-2 * epsilon)
            val disks = lcm.getPointsAndPids.asScala.map{ m =>
              val pids = m.getItems.map(_.toInt).toList.sorted
              val x = m.getX
              val y = m.getY
              val disk = geofactory.createPoint(new Coordinate(x, y))
              val userData = s"${pids.mkString(" ")}\t${global_gid}\t${taskId}"
              disk.setUserData(userData)
              disk
            }.filter(disk => grid.contains(disk.getEnvelopeInternal))

            disks.toIterator
          }
        }
      }
      results.cache()
      val N = results.count()
      n(disksPhase, N)
      (results, N)
    }
    metrics.end()
    val phase1 = getPhaseMetrics(metrics, disksPhase)

    // Collecting Tasks and RDD stats...
    val gridsDF = quadtree.getLeafZones.asScala.toVector
      .sortBy(_.partitionId)
      .map{ grid =>
        val mbr = envelope2polygon(grid.getEnvelope)
        (grid.partitionId, mbr.toText, mbr.getArea, mbr.getLength)
      }
      .toDF("grid_id", "wkt", "area", "perimeter")
    val mapTaskPartition = candidatesRaw.map{ disk =>
      val userData = disk.getUserData.toString.split("\t")
      (userData(1).toInt, userData(2).toInt)
    }.distinct().toDF("partitionId", "taskId")
      .join(gridsDF, $"partitionId" === $"grid_id")
    val phases = phase1
    val stats = mapTaskPartition.join(phases, $"taskId" === $"index")

    stats.repartition(1).write
      .mode("overwrite")
      .format("csv")
      .option("delimiter", "\t")
      .option("header", true)
      .save("hdfs:///user/acald013/logs/pflock")

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
        quadtree.getLeafZones.asScala.map{ z =>
          val id = z.partitionId
          val e = z.getEnvelope
          val p = DistanceJoin.envelope2polygon(e)
          s"${p.toText()}\t${id}\n"
        }
      }
      save{"/tmp/edgesDisks.wkt"}{
        candidatesRaw.map{ center =>
          val circle = center.buffer(epsilon / 2.0, 10)
          val pids = center.getUserData.toString
          s"${circle.toText}\t${pids}\n"
        }.collect.sorted
      }
      save{f"/tmp/PFLOCK_E${epsilon}%.0f_M1_D1.txt"}{
        candidatesRaw.map{ center =>
          val pids = center.getUserData.toString
          s"0, 0, ${pids}\n"
        }.collect.sorted
      }
      
    }
    //
    
    logger.info("Closing session...")
    logger.info(s"${appId}|${System.getProperty("sun.java.command")} --npartitions ${grids.size}")
    spark.close()
    debug{
      save{"/tmp/edgesLGrids.wkt"}{
        (0 until grids.size).flatMap{ n =>
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

