package puj

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.scala.Logging

import org.locationtech.jts.geom._

import puj.Utils._
import puj.partitioning._

object CubeTester extends Logging { 

  def main(args: Array[String]): Unit = {
    implicit var S: Settings        = Setup.getSettings(args)
    implicit val G: GeometryFactory = S.geofactory

    // Starting Spark...
    implicit val spark: SparkSession = SparkSession
      .builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .master(S.master)
      .appName("PFlock")
      .getOrCreate()

    S.appId = spark.sparkContext.applicationId
    logger.info(s"${S.appId}|START|Starting PFlocks computation")
    S.printer

    val trajs = spark.read 
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
      .cache
    trajs.count()

    implicit val ( (trajs_partitioned, cubes, quadtree), tTrajs) = timer {
      if (S.partitioner == "Fixed") {
        logger.info(s"${S.appId}|INFO|Partitioner|${S.partitioner}")
        CubePartitioner.getFixedIntervalCubes(trajs)
      } else {
        logger.info(s"${S.appId}|INFO|Partitioner|${S.partitioner}")
        CubePartitioner.getDynamicIntervalCubes(trajs)
      }
    }
    val nTrajs = trajs_partitioned.count()
    logger.info(s"${S.appId}|TIME|STPart|$tTrajs")
    logger.info(s"${S.appId}|INFO|STPart|$nTrajs")

    logger.info(s"${S.appId}|INFO|Cubes|${cubes.size}")

    save(s"/tmp/Cubes_${S.appId}.wkt") {
        cubes.values.map { cube =>
            s"${cube.wkt}\n"
        }.toList
    }
    trajs_partitioned.mapPartitionsWithIndex { (index, points) =>
        save(s"/tmp/${S.appId}_${index}.tsv", verbose = false) {  
            points.map{ point =>
                val data = point.getUserData.asInstanceOf[Data]
                val oid = data.oid
                val lon = point.getX
                val lat = point.getY
                val tid = data.tid
                s"$oid\t$lon\t$lat\t$tid\n"
            }.toList
        }
        points
    }.count()
    save(s"/tmp/CC.wkt") {
        trajs_partitioned.mapPartitionsWithIndex { (index, points) =>
            val cube   = cubes(index)
            val counts = points.size.toString()

            val wkt = cube.toText(counts)
            Iterator(wkt)
        }.collect()
    }

    logger.info(s"${S.appId}|END|PFlocks computation ended")
    spark.stop()
  }  
}
