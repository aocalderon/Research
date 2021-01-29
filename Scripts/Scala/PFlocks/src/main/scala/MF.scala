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
//import org.jgrapht.graph.{SimpleGraph, DefaultEdge}
//import org.jgrapht.Graphs

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import Utils._

object MF {

  def main(args: Array[String]) = {
    implicit val params = new Params(args)
    implicit val spark = SparkSession.builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .config("spark.kryoserializer.buffer.max", "256m")
      .appName("MF").getOrCreate()
    implicit val settings = Settings(params.epsilon(), params.mu(),
      tolerance = params.tolerance(),
      debug = params.debug(),
      appId = spark.sparkContext.applicationId,
      storageLevel = StorageLevel.MEMORY_ONLY_2
    )
    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))

    val input = params.input()
    val partitions = params.partitions()

    val (pointsRDD, cells)  = readPoints(input)
    val pairsRDD   = pairPoints(pointsRDD)
    val centersRDD = computeCenters(pairsRDD)
    val disksRDD   = getDisks(pointsRDD, centersRDD)
    val maximalsRDD= pruneDisks(disksRDD).mapPartitionsWithIndex{ (i, it) =>
      val cell = cells(i).envelope
      it.filter(point => cell.contains(point.center.getCoordinate))
    }

    if(settings.debug){
      log(s"E\t${settings.epsilon}")
      log(s"M\t${settings.mu}")
      log(s"P\t${cells.size}")

      log(s"Points  \t${pointsRDD.spatialPartitionedRDD.count}")
      log(s"Pairs   \t${pairsRDD.count}")
      log(s"Centers \t${centersRDD.rawSpatialRDD.count}")
      log(s"Disks   \t${disksRDD.count}")
      log(s"Maximals\t${maximalsRDD.count}")

      save("/tmp/edgesDisks.wkt"){
        maximalsRDD.map(_.center).mapPartitionsWithIndex{ (i, it) =>
          it.map{ disk =>
            val wkt = disk.buffer(settings.r, 25).toText
            val pids = disk.getUserData.asInstanceOf[List[Int]].sorted.mkString(" ")
            s"$wkt\t$pids\n"
          }
        }.collect
      }
      
    }

    spark.close
  }
}
