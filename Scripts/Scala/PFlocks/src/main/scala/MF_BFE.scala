package edu.ucr.dblab.pflock

import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel, Geometry}
import com.vividsolutions.jts.geom.{Geometry, Coordinate, Point}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.serializer.KryoSerializer

import org.datasyslab.geospark.spatialRDD.{CircleRDD, SpatialRDD}
import org.datasyslab.geospark.geometryObjects.Circle
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.enums.GridType

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import Utils._

object MF_BFE {
  def main(args: Array[String]) = {
    implicit val params = new Params(args)
    implicit val spark = SparkSession.builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .config("spark.kryoserializer.buffer.max", "256m")
      .appName("MF_prime").getOrCreate()
    implicit val settings = Settings(
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      tolerance = params.tolerance(),
      seed = params.seed(),
      debug = params.debug(),
      appId = spark.sparkContext.applicationId,
      storageLevel = params.storage() match {
        case 1 => StorageLevel.MEMORY_ONLY_SER_2
        case 2 => StorageLevel.NONE
        case _ => StorageLevel.MEMORY_ONLY_2  // 0 is the default...
      }
    )
    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))
    val command = System.getProperty("sun.java.command")
    log(s"$command")

    val input = params.input()
    val partitions = params.partitions()

    val (pointsRDD, cells)  = readAndReplicatePoints(input)
    
    val disksRDD = pointsRDD.mapPartitionsWithIndex{ (i, it) =>
      val points   = it.toList
      val pairs    = computePairs(points, settings.epsilon)
      val centers  = computeCenters(pairs) 
      val disks    = getDisks(points, centers)

      val cell = cells(i).envelope  
      val maximals = pruneDisks(
        disks
          .filter(point => cell.contains(point.getCoordinate))
          .map{ point =>
            val pids = point.getUserData.asInstanceOf[List[Int]]
            Disk(point, pids, List.empty[Int])
          }
      )

      maximals.toIterator
    }

    val maximals = pruneDisks(disksRDD.collect.toList)

    if(settings.debug){
      val e = settings.epsilon
      val m = settings.mu
      val p = pointsRDD.getNumPartitions
      val i = input.split("\\.").head.split("_").last
      val me = params.maxentries()

      log(s"E=${e}\tM=${m}\tP=${p}\tME=${me}\tI=${i}")
      log(s"Points   \t${pointsRDD.count}")
      log(s"Disks    \t${disksRDD.count}")
      log(s"Maximals \t${maximals.size}")

      save(s"/tmp/edgesGrids_${i}_ME${me}_E${e.toInt}_M${m}_P${p}.wkt"){
        pointsRDD.mapPartitionsWithIndex{ (i, it) =>
          val wkt = cells(i).mbr.toText 
          val n   = it.size

          Iterator(s"$wkt\t$n\t$i\n")
        }.collect 
      }
      save("/tmp/edgesPoints.wkt"){
        pointsRDD.mapPartitionsWithIndex{ (i, it) =>
          it.map{ point =>
            val wkt = point.toText
            val data = point.getUserData.asInstanceOf[Data]
            s"$wkt\t$data\t$i\n"
          }
        }.collect 
      }
    }
    save("/tmp/edgesBFE.wkt"){
      maximals.map{ maximal =>
        val wkt  = maximal.center.toText
        val pids = maximal.pids.sorted.mkString(" ")

        s"$wkt\t$pids\n"
      }
    }

    spark.close
  }
}
