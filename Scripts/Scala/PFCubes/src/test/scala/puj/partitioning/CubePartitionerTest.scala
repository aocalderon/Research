package puj.partitioning

import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, PrecisionModel}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._
import puj.{Settings, Utils}
import puj.Utils.Data
import org.apache.spark.serializer.KryoSerializer
import puj.Setup

class CubePartitionerTest extends AnyFunSuite with BeforeAndAfterAll with MockitoSugar {

  var spark: SparkSession = _
  implicit val G: GeometryFactory = new GeometryFactory(new PrecisionModel(1000.0))

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("CubePartitionerTest")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("getFixedIntervalCubes should partition trajectory points correctly") {
    implicit var S: Settings        = Setup.getSettings(Seq.empty[String]) // Initializing settings...
    implicit val G: GeometryFactory = new GeometryFactory(new PrecisionModel(S.scale)) // Setting precision model and geofactory...

    // Starting Spark...
    implicit val spark: SparkSession = SparkSession
      .builder() 
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .master(S.master)
      .appName("PFlock")
      .getOrCreate()

    S.appId = spark.sparkContext.applicationId
    S.printer

    /************************************************************************* 
    * Data reading...
    */
    val trajs = spark.read // Reading trajectories...
      .option("header", value = false)
      .option("delimiter", "\t")
      .csv(S.dataset)
      .rdd
      .mapPartitions { rows =>
        rows.map { row =>
          val oid = row.getString(0).toInt
          val lon = row.getString(1).toDouble
          val lat = row.getString(2).toDouble
          val tid = row.getString(3).toInt

          val point = G.createPoint(new Coordinate(lon, lat))
          point.setUserData(Data(oid, tid))

          (tid, point)
        }
      }
      .cache
    trajs.count()


    // Call the method under test
    val (partitionedRDD, cubes) = CubePartitioner.getFixedIntervalCubes(trajs)

    // Verify the results
    val resultPoints = partitionedRDD.collect()
    assert(resultPoints.length === trajs.count())
    assert(cubes.nonEmpty)

    // Check that the RDD is partitioned
    assert(partitionedRDD.partitioner.isDefined)
    assert(partitionedRDD.partitioner.get.numPartitions === cubes.size)
  }
}
