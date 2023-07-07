package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point}
import org.locationtech.jts.index.quadtree.{Quadtree => JTSQuadtree}

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import java.io.FileWriter

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SaveMode}

import edu.ucr.dblab.pflock.MF_Utils._
import edu.ucr.dblab.pflock.Utils._

object Tester {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params = new BFEParams(args)

    val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .master(params.master())
      .appName("Tester").getOrCreate()
    import spark.implicits._

    implicit var settings = Settings(
      dataset = params.dataset(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = "PFlocks",
      capacity = params.capacity(),
      fraction = params.fraction(),
      tolerance = params.tolerance(),
      tag = params.tag(),
      debug = params.debug(),
      output = params.output()
    )

    settings.appId = spark.sparkContext.applicationId
    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))

    printParams(args)
    log(s"START|")

    val pointsRaw = spark.read
      .textFile(settings.dataset).rdd
      .map { line =>
        val arr = line.split("\t")
        val t = arr(3).toInt// - 316 // TODO: Remove for large dataset...

        (t, line)
      }
      .partitionBy{ SimplePartitioner(654) }
      .mapPartitionsWithIndex{ case(i, it) =>
        val data = it.toList
        val n = data.size
        val f = new FileWriter(s"/home/acald013/Datasets/LA/shifted/LA_50K_T${i}.tsv")
        f.write(data.map(_._2).mkString("\n"))
        f.close
        log(s"LA_50K_T${i}|${n}")
        Iterator.empty
      }
    pointsRaw.count

    spark.close()

    log(s"END|")
  }
}
