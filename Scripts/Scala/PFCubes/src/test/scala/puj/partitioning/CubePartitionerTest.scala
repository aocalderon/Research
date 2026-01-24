package puj.partitioning

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer

import org.locationtech.jts.geom.{ Coordinate, GeometryFactory, Point, PrecisionModel }

import puj.{ Settings, Setup }
import puj.Utils.{ Data, save }

class CubePartitionerTest extends AnyFunSuite with BeforeAndAfterAll {

  def startSparkSession(implicit S: Settings): SparkSession = {
    val spark: SparkSession = SparkSession
      .builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .master(S.master)
      .appName("CubePartitionerTest")
      .getOrCreate()

    S.appId = spark.sparkContext.applicationId
    S.printer

    spark
  }

  def getData(spark: SparkSession)(implicit S: Settings): RDD[Point] = {
    val trajs = spark.read // Reading trajectories...
      .option("header", value = false)
      .option("delimiter", "\t")
      .csv(S.dataset)
      .rdd
      .mapPartitions {
        rows =>
          rows.map {
            row =>
              val oid = row.getString(0).toInt
              val lon = row.getString(1).toDouble
              val lat = row.getString(2).toDouble
              val tid = row.getString(3).toInt

              val point = S.geofactory.createPoint(new Coordinate(lon, lat))
              point.setUserData(Data(oid, tid))

              point
          }
      }.cache()
    trajs.count()
    trajs
  }

  def saveResutls(partitionedRDD: RDD[Point], cubes: Map[Int, Cube], method: String): Unit = {
    save(s"/tmp/Cubes${method}.wkt") {
      cubes.values.map {
        cube =>
          val wkt = cube.cell.wkt
          val beg = cube.interval.begin
          val dur = cube.interval.duration
          val id  = cube.id

          s"$wkt\t$id\t$beg\t$dur\n"
      }.toList
    }

    save(s"/tmp/Sample${method}.wkt") {
      partitionedRDD
        .sample(withReplacement = false, fraction = 0.1, seed = 42)
        .mapPartitionsWithIndex {
          case (index, it) =>
            val cube               = cubes(index)
            val (s_index, t_index) = Encoder.decode(cube.st_index)
            it.map {
              p =>
                val wkt  = p.toText
                val data = p.getUserData.asInstanceOf[Data]

                s"$wkt\t${data.tid}\t$s_index\t$t_index\t$index\n"
            }
        }
        .collect()
    }
  }

  test("getFixedIntervalCubes should partition trajectory points correctly") {

    // Setting up environment...
    implicit var S: Settings = Setup.getSettings(Seq.empty[String]) // Initializing settings...
    S = S.copy(master = "local[2]", eprime = 5, debug = true, step = 3, tcapacity = 100000)

    // Starting Spark...
    val spark = startSparkSession

    // Loading data...
    val trajs: RDD[Point] = getData(spark)

    val (partitionedRDD, cubes, _) = CubePartitioner.getFixedIntervalCubes(trajs)

    // Verifying the results...
    assert(cubes.nonEmpty)

    // Checking that the RDD is partitioned...
    assert(partitionedRDD.getNumPartitions === cubes.size)

    // Saving results for manual inspection...
    saveResutls(partitionedRDD, cubes, "Fixed")

    // Stopping Spark...
    spark.stop()
  }

  test("getDynamicIntervalCubes should partition trajectory points correctly") {

    // Setting up environment...
    implicit var S: Settings = Setup.getSettings(Seq.empty[String]) // Initializing settings...
    S = S.copy(master = "local[2]", eprime = 5, debug = true, tcapacity = 100000)

    // Starting Spark...
    val spark = startSparkSession

    // Loading data...
    val trajs: RDD[Point] = getData(spark)

    val (partitionedRDD, cubes, _) = CubePartitioner.getDynamicIntervalCubes(trajs)

    // Verifying the results...
    assert(cubes.nonEmpty)

    // Checking that the RDD is partitioned...
    assert(partitionedRDD.getNumPartitions === cubes.size)

    // Saving results for manual inspection...
    saveResutls(partitionedRDD, cubes, "Dynamic")

    // Stopping Spark...
    spark.stop()
  }
}
