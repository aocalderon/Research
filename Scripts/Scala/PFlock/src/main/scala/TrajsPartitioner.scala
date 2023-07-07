package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point}
import org.locationtech.jts.index.strtree.STRtree

import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop._

import scala.collection.JavaConverters._
import java.io.FileWriter

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SaveMode}

import edu.ucr.dblab.pflock.sedona.quadtree._
import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.MF_Utils._

object TrajsPartitioner {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class APoint(oid: Int, lat: Double, lon: Double, tid: Int, cid: Int = -1){
    val coordinate: Coordinate = new Coordinate(lat, lon)
    val envelope: Envelope = new Envelope(coordinate)

    def expandedEnvelope(expand: Double): Envelope = {
      val expanded = new Envelope(envelope)
      expanded.expandBy(expand)
      expanded
    }

    def getJTSPoint(implicit G: GeometryFactory): Point = {
      val point = G.createPoint(coordinate)
      point.setUserData(Data(oid, tid))
      point
    }

    def distance(other: APoint)(implicit G: GeometryFactory): Double = {
      this.getJTSPoint.distance(other.getJTSPoint)
    }
  }

  def shiftEnvelope(envelope: Envelope, shiftX: Double, shiftY: Double): Envelope = {
    new Envelope(
      envelope.getMinX - shiftX, envelope.getMaxX - shiftX,
      envelope.getMinY - shiftY, envelope.getMaxY - shiftY
    )
  }

  def main(args: Array[String]): Unit = {
    implicit val params = new TPParams(args)

    val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .master(params.master())
      .appName("TrajsPartitioner").getOrCreate()
    import spark.implicits._

    implicit val geofactory = new GeometryFactory(new PrecisionModel(1.0/params.tolerance()))

    val base_path = params.output()
    logger.info(s"Using $base_path as base path...")
    logger.info(s"START...")

    // Read points from HDFS...
    val pointsRaw = spark
      .read
      .option("header", "false")
      .option("delimiter", "\t")
      .csv(params.dataset())
      .rdd.mapPartitions{ it =>
        it.map{ row =>
          val oid = row.getString(0).toInt
          val lat = row.getString(1).toDouble
          val lon = row.getString(2).toDouble
          val tid = row.getString(3).toInt

          (tid, APoint(oid, lat, lon, tid))
        }
      }
    logger.info(s"Read points from HDFS...")

    if(params.debug()){
      println(s"pointsRaw... ${pointsRaw.count}")
      pointsRaw.take(20).foreach{println}
    }

    // Compute number of time instances to use as number of partitions...
    val times = pointsRaw.map(_._1).distinct.collect.sorted.zipWithIndex.toMap
    val pointsByTime = pointsRaw.partitionBy{ 
      MapPartitioner(times)            /** Partition the points by time instance **/
    }.map{ case(tid, point) =>
        point.copy(tid = times(tid))   /** Map original times for consecutives unit times **/
    }.cache
    pointsByTime.count
    logger.info(s"Compute number of times to use as number of partitions...")

    if(params.debug()){
      println("Times:")
      println(times.mkString(" "))
      println(s"pointsByTime... ${pointsByTime.count}")
      pointsByTime.take(20).foreach{println}
    }

    // Build the quadtree, extract cells and boundary...
    val dataset_name = params.dataset().split("/").last.split("\\.").head
    val home = System.getenv("HOME") + "/Research/local_path"
    val (cells, tree, boundary) = if(params.cached()){
      logger.info("Read the quadtree, cells and boundary from path...")
      val cells_file    = s"${home}/${base_path}/${dataset_name}/quadtree.wkt"
      val boundary_file = s"${home}/${base_path}/${dataset_name}/boundary.wkt"
      Quadtree.read(cells_file, boundary_file)
    } else {
      logger.info("Build the quadtree, cells and boundary...")
      val (cells, tree, boundary) = Quadtree.build(pointsByTime.map{_.getJTSPoint},
        capacity = params.capacity(), fraction = params.fraction())

      // Save quadtree's cells and boundary...
      save(s"${home}/${base_path}/${dataset_name}/quadtree.wkt"){
        cells.map{ case(id, envelope) =>
          val wkt = geofactory.toGeometry(envelope).toText

          s"$wkt\t$id\n"
        }.toList
      }
      save(s"${home}/${base_path}/${dataset_name}/boundary.wkt"){
        val wkt = geofactory.toGeometry(boundary).toText
        List(s"$wkt\t${dataset_name}\n")
      }
      logger.info(s"Save quadtree's cells and boundary...  ${cells.size}")

      (cells, tree, boundary)
    }

    if(params.debug()){
      println(s"Cells... ${cells.size}")
      cells.take(20).foreach{println}
    }    

    // Update the cellId attribute...
    val pointsRDD = pointsByTime.mapPartitions{ points =>
      points.flatMap{ point =>
        tree.query(point.envelope).asScala.map{ cid_prime =>
          val cid = cid_prime.asInstanceOf[Int]
    
          point.copy(cid = cid)
        }
      }
    }.cache
    logger.info("Update the cellId attribute...")

    if(params.debug()){
      println(s"pointsRDD... ${pointsRDD.count}")
      pointsRDD.take(20).foreach{println}
    }

    // Save points with updated cellId attribute...
    pointsRDD.toDS
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "false")
      .option("delimiter", "\t")
      .csv(s"${base_path}/${dataset_name}")
    logger.info("Save points with updated cellId attribute...")

    if(params.shifted()){
      // Save shifted points...
      val minX = boundary.getMinX
      val minY = boundary.getMinY
      pointsRDD.map{ point =>
        val lat = round3(point.lat - minX)
        val lon = round3(point.lon - minY)
        point.copy(lat = lat, lon = lon)
      }.toDS
        .write
        .mode(SaveMode.Overwrite)
        .option("header", "false")
        .option("delimiter", "\t")
        .csv(s"${base_path}/${dataset_name}_shifted")
      logger.info("Save shifted points...")

      // Save shifted quadtree's cells and shifted boundary...
      save(s"${home}/${base_path}/${dataset_name}_shifted/quadtree.wkt"){
        cells.map{ case(id, envelope) =>
          val wkt = geofactory.toGeometry( shiftEnvelope(envelope, minX, minY) ).toText
          s"$wkt\t$id\n"
        }.toList
      }
      save(s"${home}/${base_path}/${dataset_name}_shifted/boundary.wkt"){
        val wkt = geofactory.toGeometry( shiftEnvelope(boundary, minX, minY) ).toText
        List(s"$wkt\t${dataset_name}_shifted\n")
      }
      logger.info("Save shifted quadtree's cells and shifted boundary...")

      if(params.debug()){
        println("Boundary:")
        println(shiftEnvelope(boundary, minX, minY))
      }
    }



    spark.close()

    logger.info(s"END.")
  }
}

class TPParams(args: Seq[String]) extends ScallopConf(args) {
  val default_dataset = s"Datasets/LA/LA_50K_sample.tsv"

  val tolerance:  ScallopOption[Double]  = opt[Double]  (default = Some(1e-3))
  val dataset:    ScallopOption[String]  = opt[String]  (default = Some(default_dataset))
  val capacity:   ScallopOption[Int]     = opt[Int]     (default = Some(100))
  val fraction:   ScallopOption[Double]  = opt[Double]  (default = Some(0.05))
  val output:     ScallopOption[String]  = opt[String]  (default = Some("PFlock/LA"))
  val cached:     ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val shifted:    ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val master:     ScallopOption[String]  = opt[String]  (default = Some("local[*]"))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
