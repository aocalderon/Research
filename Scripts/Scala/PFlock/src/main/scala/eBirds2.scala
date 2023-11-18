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

object eBirds2 {
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
      .csv("/home/acald013/Datasets/eBirds/ebirds.tsv")

    val observations = data.map{ rec =>
      val oid = rec.getString(0).toLong
      val lon = rec.getString(1).toDouble
      val lat = rec.getString(2).toDouble
      val tid = rec.getString(3).toLong

      PointPrime(oid, lon, lat, tid)
    }

    val m = observations.count()
    logger.info(s"Number of observations: $m")

    val unique = observations.rdd.map{ bird =>
        val location = (bird.x, bird.y, bird.t)

        (location, bird)
    }.groupBy(_._1).map{ case(key, value) => value.map(_._2).head }.cache

    val n = unique.count()
    logger.info(s"Number of observations without duplicates: $n")

    save("/home/acald013/Datasets/eBirds/ebirds_unique_spatiotemporal_location.tsv"){
        unique.map{ bird =>
            s"${bird.oid}\t${bird.x}\t${bird.y}\t${bird.t}\n"
        }.collect
    }
    /*******************************************************************************/

    spark.close()

  }
}

