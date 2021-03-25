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
import edu.ucr.dblab.pflock.welzl.Welzl

import Utils._

object MF_Grid {
  def allocateGrid(points: List[Point], width: Double, epsilon: Double, prefix: Int = 1): List[(Int, Point, Boolean)] = {
    case class GridPoint(key: (Int, Int), flag: Boolean, point: Point)

    def digits(x: Int) = math.ceil( math.log( math.abs(x) + 1 ) / math.log(10) ).toInt

    val grid_points = points.flatMap{ point =>
      val i = math.floor(point.getX / width).toInt
      val j = math.floor(point.getY / width).toInt
      val key = (i,j)

      val data_object = List(GridPoint(key, false, point))

      val i_start = math.floor((point.getX - epsilon) / width).toInt
      val i_end   = math.floor((point.getX + epsilon) / width).toInt
      val is = i_start to i_end
      val j_start = math.floor((point.getY - epsilon) / width).toInt
      val j_end   = math.floor((point.getY + epsilon) / width).toInt
      val js = j_start to j_end

      val keys = for{
        i <- is
        j <- js
      } yield (i, j)

      val Skeys = keys.map(c => (c._1, c._2)).filterNot(k => k == key).toList

      val query_objects = Skeys.map(key => GridPoint(key, true, point))
      
      data_object ++ query_objects
    }

    val w = grid_points.map(_.key._2).max
    val grids = grid_points.map{ p =>
      val k = p.key._1 + p.key._2 * w
      (k, p.point, p.flag)
    }
    val m = grids.map(_._1).max
    val base = prefix * math.pow(10, digits(m)).toInt

    println(s"Max values is $m, so the base is $base...")

    grids.map(g => (base + g._1, g._2, g._3))
  }

  def main(args: Array[String]) = {
    implicit val params = new Params(args)
    implicit val spark = SparkSession.builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .config("spark.kryoserializer.buffer.max", "256m")
      .appName("MF_Grid").getOrCreate()
    import spark.implicits._
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
    import org.apache.spark.sql.Encoders
    val schema = Encoders.product[ST_Point].schema
    val command = System.getProperty("sun.java.command")
    log(s"$command")

    val input = params.input()
    val points = spark.read.option("header", "false").option("delimiter", "\t")
      .schema(schema).csv(input).as[ST_Point]

    log(s"Point from input: ${points.count()}")

    val width = params.width()
    val epsilon = params.epsilon()
    val pointsR = points.rdd.mapPartitionsWithIndex{ (index, it) =>
      val points = it.map{_.point}.toList
      allocateGrid(points, width, epsilon).toIterator
    }.cache

    log(s"Point replicated: ${pointsR.count()}")

    save("/tmp/edgesR.wkt"){
      pointsR.map{ p =>
        val wkt  = p._2.toText
        val key  = p._1
        val flag = p._3
        s"$wkt\t$key\t$flag\n"
      }.collect
    }

    spark.close()
  }
}
