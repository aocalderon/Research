package puj.partitioning

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, PrecisionModel}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._
import puj.{Settings, Utils}
import puj.Utils.{Data, save}
import org.apache.spark.serializer.KryoSerializer
import puj.Setup
import puj.partitioning.CubePartitioner.{getDynamicIntervalCubes => before}

class CubePartitionerTest extends AnyFunSuite with BeforeAndAfterAll with MockitoSugar {

  test("getFixedIntervalCubes should partition trajectory points correctly") {

    implicit var S: Settings = Setup.getSettings(Seq.empty[String]) // Initializing settings...
    S = S.copy(eprime = 5, debug = true, step = 3)
    implicit val G: GeometryFactory  = new GeometryFactory(new PrecisionModel(S.scale)) // Setting precision model and geofactory...

    // Starting Spark...
    val spark: SparkSession = SparkSession
      .builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .master(S.master)
      .appName("CubeTester")
      .getOrCreate()

    S.appId = spark.sparkContext.applicationId
    S.printer

    val trajs: RDD[Point] = spark.read // Reading trajectories...
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

          point
        }
      }
    val nTrajs = trajs.count()
    val (partitionedRDD, cubes, _) = if(false) CubePartitioner.getFixedIntervalCubes(trajs) else CubePartitioner.getDynamicIntervalCubes(trajs)

    save("/tmp/CubesF.wkt") {
      cubes.values.map { cube =>
        val wkt = cube.cell.wkt
        val beg = cube.interval.begin
        val dur = cube.interval.duration
        val id  = cube.id

        s"$wkt\t$id\t$beg\t$dur\n"
      }.toList
    }

    save("/tmp/SampleF.wkt") {
      partitionedRDD
        .sample(withReplacement = false, fraction = 0.1, seed = 42)
        .mapPartitionsWithIndex{ case(index, it) =>
          val cube = cubes(index)
          val (s_index, t_index) = Encoder.decode(cube.st_index)
          it.map{ p =>
            val wkt = p.toText
            val data = p.getUserData.asInstanceOf[Data]

            s"$wkt\t${data.tid}\t$s_index\t$t_index\t$index\n"
          }
        }
        .collect()
     }

    // Verify the results
    assert(cubes.nonEmpty)

    // Check that the RDD is partitioned
    assert(partitionedRDD.getNumPartitions === cubes.size)

    spark.stop()

  }

  /*
  test("getDynamicIntervalCubes should partition trajectory points correctly") {
    val (partitionedRDD, cubes, _) = CubePartitioner.getDynamicIntervalCubes(trajs)

    save("/tmp/CubesD.wkt") {
      cubes.values.map { cube =>
        val wkt = cube.cell.wkt
        val beg = cube.interval.begin
        val dur = cube.interval.duration
        val id  = cube.id

        s"$wkt\t$id\t$beg\t$dur\n"
      }.toList
    }

    save("/tmp/SampleD.wkt") {
      partitionedRDD
        .sample(withReplacement = false, fraction = 0.1, seed = 42)
        .mapPartitionsWithIndex{ case(index, it) =>
          val cube = cubes(index)
          val (s_index, t_index) = Encoder.decode(cube.st_index)
          it.map{ p =>
            val wkt = p.toText
            val data = p.getUserData.asInstanceOf[Data]

            s"$wkt\t${data.tid}\t$s_index\t$t_index\t$index\n"
          }
        }
        .collect()
    }

    // Verify the results
    assert(cubes.nonEmpty)

    // Check that the RDD is partitioned
    assert(partitionedRDD.getNumPartitions === cubes.size)
   }
   */
}
