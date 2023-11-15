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

    val sample = data.map{ rec =>
      val x = rec.getString(0).toDouble
      val y = rec.getString(1).toDouble
      val i = rec.getString(2).split(":").last
      val d = rec.getString(3)
      val t = if(rec.getString(4) == null) "00:00:00" else rec.getString(4)
      val datetime = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:SS").parseDateTime(s"${d} ${t}")
      val millis = datetime.getMillis()
      val joda_time = new JodaDateTime(millis)

      s"$i\t$x\t$y\t$millis\t$d $t\t${joda_time}"
    }.distinct()

    sample.show(truncate = false)

    /*******************************************************************************/

    spark.close()

  }
}
