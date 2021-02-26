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
import org.jgrapht.graph.{SimpleGraph, DefaultEdge}
import org.jgrapht.Graphs

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import edu.ucr.dblab.pflock.pbk.PBK.bk
import edu.ucr.dblab.pflock.pbk.PBK_Utils.getEdges

import Utils._

object MF_CMBC {
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

    log(s"Points after replication: ${pointsRDD.count}")
    log(s"Number of partitions: ${cells.size}")

    val rdd = pointsRDD.mapPartitionsWithIndex{ (i, it) =>
      val vertices = it.toList
      val edges = getEdges(vertices, settings.epsilon)
      val cliques = bk(vertices, edges)

      cliques.iterator.filter(_.size >= settings.mu)
    }
    log(s"Number of cliques: ${rdd.count}")

    spark.close
  }
}
