import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Geometry, Coordinate, Envelope, Point}
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, CircleRDD, PointRDD}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialPartitioning.KDBTree
import org.datasyslab.geospark.spatialPartitioning.quadtree.{QuadTreePartitioner, StandardQuadTree, QuadRectangle}
import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop._
import org.andress.Utils._

object RedundantRemover {
  implicit val logger = LoggerFactory.getLogger("myLogger")
  val geofactory: GeometryFactory = new GeometryFactory()
  val precision = 0.001

  def main(args: Array[String]): Unit = {

    val params = new RRConf(args)
    implicit val d = params.debug()
    val gridType: GridType = GridType.QUADTREE

    val spark = SparkSession.builder()
      .config("spark.default.parallelism", 3 * params.cores() * params.executors())
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.executor.cores", params.cores())
      .config("spark.cores.max", params.cores() * params.executors())
      .appName("RedundantRemover")
      .getOrCreate()
    import spark.implicits._
    val config = spark.sparkContext.getConf.getAll.mkString("\n")
    logger.info(config)

    val flocks = timer{"Reading data"}{
      spark.read.option("header", false).option("delimiter", "\t").csv(params.input())
        .rdd.map{ row =>
          val i = row.getString(0).split(" ").map(_.toInt).toVector
          val s = row.getString(1).toInt
          val e = row.getString(2).toInt
          val x = row.getString(3).toDouble
          val y = row.getString(4).toDouble

          val p = geofactory.createPoint(new Coordinate(x, y))

          Flock(i, s, e, p)
        }.cache
    }

    debug{
      logger.info(s"Number of flocks: ${flocks.count()}")
    }

    val flocksRDD = timer{"Partitioning data"}{
      val flocksRDD = new SpatialRDD[Point]()
      flocksRDD.setRawSpatialRDD( flocks.map{ flock =>
        flock.center.setUserData(flock.toString())
        flock.center
      })
      flocksRDD.analyze()
      flocksRDD.spatialPartitioning(GridType.QUADTREE, params.partitions())
      flocksRDD.spatialPartitionedRDD.cache

      flocksRDD
    }

    debug{
      val n = flocksRDD.spatialPartitionedRDD.getNumPartitions
      logger.info(s"Number of partitions: $n")
    }

    spark.close()
  }
}

class RRConf(args: Seq[String]) extends ScallopConf(args) {
  val input = opt[String](default = Some("/home/acald013/Datasets/ValidationLogs/redundants.txt"))
  val cores = opt[Int](default = Some(4))
  val executors = opt[Int](default = Some(10))
  val partitions = opt[Int](default = Some(256))
  val gridType = opt[String](default = Some("QUADTREE"))
  val debug = opt[Boolean](default = Some(false))

  verify()
}
