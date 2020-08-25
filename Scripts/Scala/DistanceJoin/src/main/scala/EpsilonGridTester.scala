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
import DisksFinder._

object EpsilonGridTester {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class ST_Point(id: Int, x: Double, y: Double, t: Int){
    def asWKT: String = s"POINT($x $y)\t$id\t$t\n"
    def distance(p: ST_Point): Double = {
      math.sqrt(math.pow(x - p.x, 2) - math.pow(y - p.y, 2))
    }
  }

  case class Index(i: Int, j: Int)

  def main(args: Array[String]): Unit = {
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

    val readingStage = "Reading points"
    val (pointsRaw, nPoints, boundary) = timer{readingStage}{
      val pointsSchema = ScalaReflection.schemaFor[ST_Point]
        .dataType.asInstanceOf[StructType]
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
      val boundary = pointsRaw.boundary()
      pointsRaw.analyze(boundary, nPointsRaw.toInt)
      pointsRaw.rawSpatialRDD.persist(persistanceLevel)
      n(readingStage, nPointsRaw)
      (pointsRaw, nPointsRaw, boundary)
    }

    val partitionStage = "Partitions done"
    val (pointsRDD, nPointsRDD, quadtree) = timer{partitionStage}{
      val fraction = FractionCalculator.getFraction(params.partitions(), nPoints)
      logger.info(s"Fraction: ${fraction}")
      val samples = pointsRaw.getRawSpatialRDD.rdd.sample(false, fraction, 42)
        .map(_.getEnvelopeInternal).collect().toList.asJava
      val partitioning = new QuadtreePartitioning(samples,
        pointsRaw.boundary(),
        params.partitions())
      val quadtree = partitioning.getPartitionTree()
      val partitioner = new QuadTreePartitioner(quadtree)

      val points = new CircleRDD(pointsRaw, 0.0)
      points.spatialPartitioning(partitioner)
      points.buildIndex(IndexType.QUADTREE, true)
      //pointsRaw.spatialPartitioning(GridType.QUADTREE, params.partitions())

      points.indexedRDD.persist(persistanceLevel)
      val N = points.spatialPartitionedRDD.count()
      n(partitionStage, N)
      (points, N, quadtree)
    }

    val overlapingGridStage = "Grid done"
    val (gridsRDD, nGridsRDD) = timer{overlapingGridStage}{
      val x = boundary.getMinX + epsilon / 2.0
      val y = boundary.getMinY + epsilon / 2.0

      val grids = for{
        i <- x until boundary.getMaxX by epsilon
        j <- y until boundary.getMaxY by epsilon
      } yield {
        val centroid = new Coordinate(i, j)
        val envelope = new Envelope(centroid)
        envelope.expandBy(1.5 * epsilon)

        val grid = envelope2polygon(envelope)
        val index = Index((i / epsilon).toInt, (j / epsilon).toInt)
        grid.setUserData(index)
        grid
      }

      val gridsRDD = new SpatialRDD[Polygon]()
      gridsRDD.setRawSpatialRDD(spark.sparkContext.parallelize(grids, params.partitions()))
      gridsRDD.spatialPartitioning(pointsRDD.getPartitioner)
      gridsRDD.spatialPartitionedRDD.persist(persistanceLevel)

      val N = gridsRDD.spatialPartitionedRDD.count()
      n(overlapingGridStage, N)
      (gridsRDD, N)
    }

    val joinStage = "Join Done"
    val (partsRDD, nPartsRDD) = timer{joinStage}{
      val r = JoinQuery.DistanceJoinQueryFlat(gridsRDD, pointsRDD, true, true)

      val partsRDD = r.rdd.map{ case(g, p) =>
        val gid = p//.getUserData.asInstanceOf[Index]
        val point = g.asInstanceOf[Point]

        (gid, Vector(point))
      }.reduceByKey( (l, r) => l ++ r).filter(_._2.length > 1)

      partsRDD.persist(persistanceLevel)
      val N = partsRDD.count()
      n(joinStage, N)

      (partsRDD, N)
    }

    /******************************************************/
    debug{
      save{"/tmp/edgesCorners.wkt"}{
        partsRDD.map{ part =>
          val wkt = part._1.toText
          s"$wkt\n"
        }.collect
      }
      save{"/tmp/edgesPoints.wkt"}{
        partsRDD.flatMap{ part =>
          val index = part._1.getUserData.asInstanceOf[Index]
          val i = index.i
          val j = index.j

          val points = part._2
          points.map{ point =>
            val wkt = point.toText

            s"$wkt\t$i $j\n"
          }
        }.collect
      }
    }
    /******************************************************/

    val precision = params.precision()
    val disksStage = "Disk found"
    val (disksRDD, nDisksRDD) = timer{disksStage}{
      val disks = partsRDD.map{ case(grid, points) =>
        val st_points = points.map{ p =>
          val id = p.getUserData.toString.split("\t")(0)
          (id, p)
        }
        val pairs = for{
          a <- st_points
          b <- st_points if a._1 < b._1 && a._2.distance(b._2) <= epsilon + precision 
        } yield {
          (a._2, b._2)
        }

        val innerArea = grid.getEnvelopeInternal
        innerArea.expandBy(-1 * epsilon)
        val centers = pairs.flatMap{ case(a, b) =>
          calculateCenterCoordinates(a, b, r2)
        }.filter(center => innerArea.intersects(center.getEnvelopeInternal))

        val disksFlat = for{
          c <- centers
          p <- points if c.distance(p) <= epsilon / 2.0 + precision 
        } yield {
          (c, p.getUserData.toString.split("\t")(0))
        }
        val disks = disksFlat.groupBy(_._1)
          .map{ case(center, points) =>
            val pids = points.map(_._2.toInt).sorted
            (center, pids)
          }

        (grid, pairs, centers, points.size, disks)
      }

      disks.persist(persistanceLevel)
      val N = disks.count()
      n(disksStage, N)

      (disks, N)
    }

    debug{
      save{"/tmp/edgesEGrids.wkt"}{
        disksRDD.map{ disk =>
          val grid = disk._1
          val innerArea = grid.getEnvelopeInternal
          innerArea.expandBy(-1 * epsilon)
          val wkt = envelope2polygon(innerArea).toText
          val count = disk._4

          s"$wkt\t$count\n"
        }.collect
      }
      save{"/tmp/edgesPairs.wkt"}{
        disksRDD.flatMap{ disk =>
          val grid = disk._1
          val innerArea = grid.getEnvelopeInternal
          innerArea.expandBy(-1 * epsilon)
          val area = envelope2polygon(innerArea)

          val pairs = disk._2
          pairs.map{ pair =>
            val p1 = pair._1.getCoordinate
            val p2 = pair._2.getCoordinate
            geofactory.createLineString(Array(p1, p2))
          }.filter(pair => area.intersects(pair))
          .map{ pair =>
             s"${pair.toText}\n"
          }
        }.distinct.collect
      }
      save{"/tmp/edgesDisks.wkt"}{
        disksRDD.flatMap{disk =>
          val disks = disk._5
          disks.map{ d =>
            val wkt  = d._1.toText
            val pids = d._2.mkString(" ")

            s"$wkt\t$pids\n"
          }
        }.collect
      }
    }
  }
}
