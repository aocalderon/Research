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

object MF_prime {
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

    val (pointsRDD, cells)  = readAndReplicatePoints(input, partitions)
    
    val disksRDD = pointsRDD.mapPartitions{ it =>
      val points  = it.toList
      val pairs   = computePairs(points, settings.epsilon)
      val centers = computeCenters(pairs) 
      val disks   = getDisks(points, centers)

      disks.toIterator
    }

    if(settings.debug){
      saveCells(cells.values.toList)
      log(s"E\t${settings.epsilon}")
      log(s"M\t${settings.mu}")
      log(s"P\t${pointsRDD.getNumPartitions}")

      log(s"Points \t${pointsRDD.count}")
      log(s"Disks  \t${disksRDD.count}")
    }

    spark.close
  }
}