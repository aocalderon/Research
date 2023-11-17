package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point}

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import java.io.FileWriter

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SaveMode}

import edu.ucr.dblab.pflock.MF_Utils._
import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.sedona.quadtree.Quadtree

import com.github.nscala_time.time.Imports._
import org.joda.time.{DateTime => JodaDateTime}

import org.locationtech.proj4j.CRSFactory;
import org.locationtech.proj4j.CoordinateReferenceSystem;
import org.locationtech.proj4j.CoordinateTransform;
import org.locationtech.proj4j.CoordinateTransformFactory;
import org.locationtech.proj4j.ProjCoordinate;

object eBirds {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {

    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .master("local[*]")
      .appName("Tester").getOrCreate()
    import spark.implicits._

    implicit val G = new GeometryFactory(new PrecisionModel(1e6))

    /*******************************************************************************/
    // Code here...
    
    val data = spark.read.option("header", false).option("sep", "\t").csv("/home/acald013/Datasets/eBirds/sample.tsv")

    data.show(truncate = false)

    val crsFactory = new CRSFactory();
    val epsg4326 = crsFactory.createFromName("epsg:4326");
    val epsg6423 = crsFactory.createFromName("epsg:6426");

    val sample = data.map{ rec =>
      val lon = rec.getString(0).toDouble
      val lat = rec.getString(1).toDouble
      val pid = rec.getString(2).split(":").last
      val did = rec.getString(3)
      val tid = if(rec.getString(4) == null) "00:00:00" else rec.getString(4)

      val ms = DateTimeFormat
        .forPattern("YYYY-MM-dd HH:mm:SS")
        .parseDateTime(s"${did} ${tid}")
        .getMillis()

      val ctFactory = new CoordinateTransformFactory().createTransform(epsg4326, epsg6423)
      val coord = new ProjCoordinate()
      ctFactory.transform(new ProjCoordinate(lon, lat), coord)
      val x = coord.x
      val y = coord.y

      s"$pid\t$x\t$y\t$ms"
    }.distinct()

    sample.show(truncate = false)

    /*******************************************************************************/

    spark.close()

  }
}
