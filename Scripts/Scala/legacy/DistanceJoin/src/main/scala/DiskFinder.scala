package edu.ucr.dblab.djoin

import org.slf4j.{LoggerFactory, Logger}
import scala.collection.JavaConverters._
import ch.cern.sparkmeasure.TaskMetrics

import org.apache.spark.TaskContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, Dataset, Row}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions

import org.datasyslab.geospark.spatialRDD.{SpatialRDD, CircleRDD, PointRDD}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.geosparkviz.sql.utils.GeoSparkVizRegistrator
import org.datasyslab.geosparksql.utils.Adapter

import com.vividsolutions.jts.index.SpatialIndex
import com.vividsolutions.jts.index.quadtree._
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate}
import com.vividsolutions.jts.geom.{Point, Polygon, MultiPolygon}

import edu.ucr.dblab.Utils._
import edu.ucr.dblab.djoin.DistanceJoin.{geofactory, round, circle2point, envelope2polygon}
import edu.ucr.dblab.djoin.SPMF.{AlgoLCM2, Transactions, Transaction}
import Stats._

object DisksFinder{
  case class ST_Point(id: Int, x: Double, y: Double, t: Int){
    def asWKT: String = s"POINT($x $y)\t$id\t$t\n"
  }

  def main(args: Array[String]): Unit = {
    implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
    logger.info("Starting session...")
    implicit val params = new DiskFinderConf(args)
    val appName = s"DiskFinder"
    implicit val debugOn = params.debug()
    implicit val spark = SparkSession.builder()
      .appName(appName)
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
      .getOrCreate()
    GeoSparkSQLRegistrator.registerAll(spark)
    GeoSparkVizRegistrator.registerAll(spark)
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
      }
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
    val (pointsRDD, nPointsRDD, quadtree) = timer{partitionStage}{
      val fraction = FractionCalculator.getFraction(params.partitions(), nPoints)
      logger.info(s"Fraction: ${fraction}")
      val samples = pointsRaw.getRawSpatialRDD.rdd.sample(false, fraction, 42)
        .map(_.getEnvelopeInternal).collect().toList.asJava
      val partitioning = new QuadtreePartitioning(samples,
        pointsRaw.boundary(),
        params.partitions(),
        params.epsilon(),
        params.factor())
      val quadtree = partitioning.getPartitionTree()
      val partitioner = new QuadTreePartitioner(quadtree)
      pointsRaw.spatialPartitioning(partitioner)
      //pointsRaw.spatialPartitioning(GridType.QUADTREE, params.partitions())

      pointsRaw.spatialPartitionedRDD.persist(persistanceLevel)
      val N = pointsRaw.spatialPartitionedRDD.count()
      n(partitionStage, N)
      (pointsRaw, N, quadtree)
    }

    debug{
      case class Cell(id: Int, count: Int = -1, wkt: String = ""){
        override def toString = s"$wkt\t$id\t$count\n"
      }

      val partitions = pointsRDD.spatialPartitionedRDD.rdd
        .mapPartitionsWithIndex{ (index, it) =>
        Iterator(Cell(index, count = it.length))
      }.collect
      val counts = partitions.map(_.count)
      val avg = mean(counts)
      val max = counts.max
      val min = counts.min
      val sd  = stdDev(counts)
      logger.info(s"STATS|$appId|$max|$min|$avg|$sd")

      val cells = quadtree.getLeafZones.asScala.map{ leaf =>
        val id = leaf.partitionId
        val wkt = envelope2polygon(leaf.getEnvelope).toText
        Cell(id, wkt = wkt)
      }

      val data = for{
        p <- partitions
        c <- cells if p.id == c.id
      } yield {
        Cell(p.id, p.count, c.wkt)
      }

      save{s"/tmp/edgesCells$appId.tsv"}{
        data.map(_.toString() + s"$appId")
      }
    }

    // Collecting the global settings...
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

    ////
    case class CellStats(partitionId: Int, taskId: Long,
      pointsIn: Int, pointsOut: Int,
      centersIn: Int, centersOut: Int,
      disks: Vector[Point],
      centers: Vector[Point]){
      override def toString = s"$partitionId\t$taskId\t$pointsIn\t$pointsOut\t$centersIn\t$centersOut\n"
    }
    ////

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

            // Finding pairs...
            val points  = leftIt.toVector
            val pairs = DistanceJoin.partitionBasedIterator(
              points.toIterator,
              rightIt,
              epsilon
            ).filter{ case(l,r) =>
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

            ////
            val pIn  = points.filter(p => grid.contains(p.getEnvelopeInternal)).length
            val pOut = points.filter(p => !grid.contains(p.getEnvelopeInternal)).length
            val cIn  = centers.filter(c => grid.contains(c.getEnvelopeInternal)).length
            val cOut = centers.filter(c => !grid.contains(c.getEnvelopeInternal)).length

            val cell = CellStats(global_gid, taskId, pIn, pOut, cIn, cOut,
              disks.toVector,
              centers.toVector
            )
            Iterator(cell)
            ////

            //disks.toIterator // Keep it to return just the disks...
          }
        }
      }
      results.cache()
      logger.info("\n" + results.map{c =>
        s"CELLSTATS\t$appId\t${c.toString}"
      }.collect.mkString("")
      )
      save{"/tmp/edgesCenters.wkt"}{
        results.flatMap(_.centers).map{ center =>
          s"${center.toText}\n"
        }.collect
      }
      val disks = results.flatMap(_.disks)
      val N = disks.count()
      n(disksPhase, N)
      (disks, N)
    }
    metrics.end()
    val phase1 = getPhaseMetrics(metrics, disksPhase)

    val metrics_output = s"hdfs:///user/acald013/logs/pflock-${appId}"
    logger.info(s"Saving metrics at ${metrics_output}...")
    val phases = phase1
    phases.repartition(1).write
      .mode("overwrite")
      .format("csv")
      .option("delimiter", "\t")
      .option("header", true)
      .save(metrics_output)
    logger.info(s"Saving metrics at ${metrics_output}... Done!")

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

