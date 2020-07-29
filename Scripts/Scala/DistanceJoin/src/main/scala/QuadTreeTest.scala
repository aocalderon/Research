package edu.ucr.dblab.djoin

import org.slf4j.{LoggerFactory, Logger}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.{SpatialRDD}
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.geom.{Geometry, Envelope, Polygon,  Coordinate, Point}
import scala.collection.JavaConverters._
import edu.ucr.dblab.Utils._
import ch.cern.sparkmeasure.TaskMetrics
import org.apache.spark.util.random.SamplingUtils

object QuadTreeTest {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
  val scale: Double = 1000.0
  val model: PrecisionModel = new PrecisionModel(scale)
  val geofactory: GeometryFactory = new GeometryFactory(model)

  def envelope2polygon(e: Envelope): Polygon = {
    val minX = e.getMinX()
    val minY = e.getMinY()
    val maxX = e.getMaxX()
    val maxY = e.getMaxY()
    val p1 = new Coordinate(minX, minY)
    val p2 = new Coordinate(minX, maxY)
    val p3 = new Coordinate(maxX, maxY)
    val p4 = new Coordinate(maxX, minY)
    geofactory.createPolygon( Array(p1,p2,p3,p4,p1))
  }

  def main(args: Array[String]): Unit = {
    logger.info("Starting session...")
    implicit val params = new QuadTreeTestConf(args)
    val appName = s"QuadTreeTest"
    implicit val spark = SparkSession.builder()
      .appName(appName)
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    GeoSparkSQLRegistrator.registerAll(spark)
    val appId = spark.sparkContext.getConf.get("spark.app.id")
    val command = System.getProperty("sun.java.command")
    logger.info(s"${appId}|${command}")
    import spark.implicits._
    logger.info("Starting session... Done!")

    val input = params.input()
    logger.info(s"Reading ${input}...")
    val pointsJTS = spark.read
      .option("header", false)
      .option("delimiter", "\t")
      .csv(input)
      .rdd.map{ wkt =>
        val x = wkt.getString(1).toDouble
        val y = wkt.getString(2).toDouble

        geofactory.createPoint(new Coordinate(x, y))
      }.cache
    val pointsRDD = new SpatialRDD[Point]()
    pointsRDD.setRawSpatialRDD(pointsJTS)
    val nPointsRDD = pointsRDD.countWithoutDuplicates()
    logger.info(s"Reading ${input}... Done!")
    logger.info(s"Number of records: ${nPointsRDD}")

    logger.info("Partitioning...")
    pointsRDD.analyze()
    val fraction = FractionCalculator.getFraction(params.partitions(), nPointsRDD)
    logger.info(s"Fraction: ${fraction}")
    val samples = pointsJTS.sample(false, fraction, 42)
      .map(_.getEnvelopeInternal).collect().toList.asJava
    val partitioning = new QuadtreePartitioning(
      samples,
      pointsRDD.boundary(),
      params.partitions(),
      params.epsilon(),
      params.factor())
    val quadtree = partitioning.getPartitionTree()
    val partitioner = new QuadTreePartitioner(quadtree)
    pointsRDD.spatialPartitioning(partitioner)
    val points = pointsRDD.spatialPartitionedRDD.rdd
    points.cache()
    points.count()
    logger.info("Partitioning... Done!")
    
    logger.info(s"Saving...")
    save{"/tmp/edgesGrids.wkt"}{
      quadtree.getLeafZones.asScala.map{ leaf =>
        val id = leaf.partitionId
        val mbr = leaf.getEnvelope()
        val polygon = envelope2polygon(mbr)
        s"${polygon.toText()}\t${id}\n"
      }
    }
    save{"/tmp/edgesPoints.wkt"}{
      pointsRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ (index, points) =>
        points.map{ point =>
          s"${point.toText()}\t${index}\n"
        }
      }.collect
    }
    logger.info(s"Saving... Done!")

    logger.info("Closing session...")
    spark.close()
    logger.info("Closing session... Done!")
  }
}

import org.rogach.scallop.ScallopConf

class QuadTreeTestConf(args: Seq[String]) extends ScallopConf(args) {
  val input      = opt[String](default = Some(""))
  val epsilon    = opt[Double](default = Some(10.0))
  val partitions = opt[Int](default = Some(256))
  val factor     = opt[Int](default = Some(4))
  val capacity   = opt[Int](default = Some(20))
  val fraction   = opt[Double](default = Some(0.01))
  val levels     = opt[Int](default = Some(5))

  verify()
}
