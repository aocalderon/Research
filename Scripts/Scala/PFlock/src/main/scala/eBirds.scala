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

case class PointPrime(oid: Long, x: Double, y: Double, t: Long){
  var G: GeometryFactory = new GeometryFactory()
  val coord: Coordinate = new Coordinate(x, y)
  val text: String = s"$oid\t$x\t$y\t$t\n"

  def asPoint: Point = {
    val point = G.createPoint(coord)
    point.setUserData(s"$oid\t$t")
    point
  }


}

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
    
    val data = spark.read
      .option("header", false)
      .option("sep", "\t")
      .csv("/home/acald013/Datasets/eBirds/ebirds_ca.tsv")

    val crsFactory = new CRSFactory();
    val epsg4326 = crsFactory.createFromName("epsg:4326");
    val epsg6423 = crsFactory.createFromName("epsg:3857");

    val observations = data.map{ rec =>
      val lon = rec.getString(0).toDouble
      val lat = rec.getString(1).toDouble
      val oid = rec.getString(2).split(":").last
      val did = rec.getString(3)
      val tid = if(rec.getString(4) == null) "00:00:00" else rec.getString(4)

      val epoch = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:SS").parseDateTime(s"${did} ${tid}")

      val ctFactory = new CoordinateTransformFactory().createTransform(epsg4326, epsg6423)
      val coord = new ProjCoordinate()
      ctFactory.transform(new ProjCoordinate(lon, lat), coord)
      
      val i = oid.substring(3).toLong
      val x = coord.x
      val y = coord.y
      val t = epoch.getMillis()

      PointPrime(i, x, y, t)
    }

    println(observations.count())

    val f = new FileWriter("/home/acald013/Datasets/eBirds/ebirds_sample.tsv")
    observations.sample(0.01, 42L).collect.foreach{ bird =>
      f.write(s"${bird.oid}\t${bird.x}\t${bird.y}\t${bird.t}\n")
    }    
    f.close

    /*******************************************************************************/

    spark.close()

  }
}
