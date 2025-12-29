package puj.partitioning

import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, PrecisionModel}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._
import puj.{Settings, Utils}
import puj.Utils.{Data, save}
import org.apache.spark.serializer.KryoSerializer
import puj.Setup

class CubePartitionerTest extends AnyFunSuite with BeforeAndAfterAll with MockitoSugar {

  test("getFixedIntervalCubes should partition trajectory points correctly") {
    implicit var S: Settings = Setup.getSettings(Seq.empty[String]) // Initializing settings...
    S = S.copy(eprime = 5, debug = true, step = 3)
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

    /** *********************************************************************** Data reading...
      */
    val trajs = spark.read // Reading trajectories...
      .option("header", value = false)
      .option("delimiter", "\t")
      .csv(S.dataset)
      .rdd
      // .sample(withReplacement=false, fraction=0.1, seed=42)
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
    val nTrajs = trajs.count()

    // Call the method under test
    val (partitionedRDD, cubes, _) = CubePartitioner.getFixedIntervalCubes(trajs)

    save("/tmp/Cubes.wkt") {
      cubes.values.map { cube =>
        val wkt = cube.cell.wkt
        val beg = cube.interval.begin
        val dur = cube.interval.duration
        val id  = cube.id

        s"$wkt\t$id\t$beg\t$dur\n"
      }.toList
    }

    // Verify the results
    assert(cubes.nonEmpty)

    // Check that the RDD is partitioned
    assert(partitionedRDD.getNumPartitions === cubes.size)

    spark.stop()

  }
}
