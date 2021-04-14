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

object MF_CMBC {
  def main(args: Array[String]) = {
    implicit val params = new Params(args)
    implicit val spark = SparkSession.builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .config("spark.kryoserializer.buffer.max", "256m")
      .appName("MF_CMBC").getOrCreate()
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
      val vertices = it.toList
      val edges = getEdges(vertices, settings.epsilon)
      val cliques = bk(vertices, edges).iterator
        .filter(_.size >= settings.mu) // Filter cliques with no enough points...

      val (maximals_prime, disks_prime) = cliques.map{ points =>
        val mbc = Welzl.mbc(points)

        (mbc, points)
      }.partition{ case (mbc, points) => // Split cliques enclosed by a single disk...
          round(mbc.getRadius) < settings.r
      }

      val maximals1 = maximals_prime.map{ case(mbc, points) =>
        val pids = points.map(_.getUserData.asInstanceOf[Data].id)
        val center = geofactory.createPoint(new Coordinate(mbc.getCenter.getX,
          mbc.getCenter.getY))

        Disk(center, pids, List.empty)
      }.toList

      val maximals2 = disks_prime.map{ case(mbc, points) =>
        val centers = computeCenters(computePairs(points, settings.epsilon))
        val disks = getDisks(points, centers).map{ p =>
          val pids = p.getUserData.asInstanceOf[List[Long]]
          Disk(p, pids, List.empty[Int])
        }
        pruneDisks(disks)
      }.toList.flatten

      val maximals = pruneDisks(maximals1 ++ maximals2)

      maximals.toIterator
    }

    val maximals = pruneDisks(disksRDD.collect.toList)

    if(settings.debug){
      val e = settings.epsilon
      val m = settings.mu
      val p = pointsRDD.getNumPartitions
      val i = input.split("\\.").head.split("/").last
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
    save("/tmp/edgesCMBC.wkt"){
      maximals.map{ maximal =>
        val wkt  = maximal.center.toText
        val pids = maximal.pids.sorted.mkString(" ")

        s"$wkt\t$pids\n"
      }
    }
    
    spark.close
  }
}
