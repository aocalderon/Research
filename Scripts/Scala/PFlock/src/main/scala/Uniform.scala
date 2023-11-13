package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point, LineString}
import org.locationtech.jts.index.quadtree.{Quadtree => JTSQuadtree}
import org.locationtech.jts.index.strtree.STRtree
import org.locationtech.jts.io.WKTReader

import org.slf4j.{Logger, LoggerFactory}

import scala.xml._
import scala.collection.JavaConverters._
import java.io.FileWriter

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SaveMode}

import edu.ucr.dblab.pflock.MF_Utils._
import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.sedona.quadtree.Quadtree

object Uniform {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params = new BFEParams(args)

    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .master(params.master())
      .appName("Tester").getOrCreate()
    import spark.implicits._

    implicit var S = Settings(
      dataset = params.dataset(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = "PFlocks",
      capacity = params.capacity(),
      fraction = params.fraction(),
      tolerance = params.tolerance(),
      tag = params.tag(),
      debug = params.debug(),
      output = params.output(),
      appId = spark.sparkContext.applicationId
    )

    implicit val geofactory = new GeometryFactory(new PrecisionModel(S.scale))

    printParams(args)
    log(s"START|")

    /*******************************************************************************/
    // Code here...

    val factor = 1e6
    val pointsRDD = spark.read
      .option("header", false)
      .option("separator", "\t")
      .csv(s"uniform/uniform_${params.dataset()}K.wkt")
      .rdd
      .zipWithIndex()
      .mapPartitions{ rows =>
        val reader = new WKTReader(geofactory)
        rows.map{ case(row, id) =>
          val wkt = row.getString(0)
          val point = reader.read(wkt).asInstanceOf[Point]
          point.setUserData(Data(id.toInt, 0))

          point
        }
      }

    val dataset  = params.dataset()
    val fraction = params.fraction()
    val capacity = params.capacity()

    val (cells, tree, mbr) = Quadtree.build(pointsRDD, fraction = fraction, capacity = capacity)


    val pointset = pointsRDD.map{ point =>
      val envelope = point.getEnvelopeInternal

      val cid = tree.query(envelope).asScala.map { _.asInstanceOf[Int] }.head

      (cid, STPoint(point, cid))
    }
      .partitionBy(SimplePartitioner(cells.size))
      .map{_._2}
      .mapPartitionsWithIndex{ case(cid, it) =>
        val polygon = geofactory.toGeometry(cells(cid))
        val wkt     = polygon.toText
        val count   = it.size
        val area    = polygon.getArea
        val density = count / area

        Iterator(s"$wkt\t$cid\t$count\t$area\t$density\n")
      }

    val qsize = cells.size
    log(s"Number of cells:\t$qsize")

    val base_path = "/home/acald013/Research/Datasets/uniform/cells"
    val file_name = s"${base_path}/cells_${dataset}K_C${capacity}_Q${qsize}.wkt"

    save(file_name){ pointset.collect() }

    /*
    pointset.mapPartitionsWithIndex{ case(cid, it) =>
      val points = it.toList

      S = S.copy(method="BFE")
      val (maximalsBFE, stats1) = BFE.run(points)
      stats1.print()
      S = S.copy(method="PSI")
      val (maximalsPSI, stats2) = PSI.run(points)
      stats2.printPSI()

      it
     }.collect()
     */


    /*******************************************************************************/

    spark.close()

    log(s"END|")
  }
}
