package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point, LineString}
import org.locationtech.jts.index.quadtree.{Quadtree => JTSQuadtree}
import org.locationtech.jts.index.strtree.STRtree
import org.locationtech.jts.io.WKTReader

import org.slf4j.{Logger, LoggerFactory}

import scala.xml._
import scala.collection.JavaConverters._
import scala.annotation.tailrec
import java.io.FileWriter

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SaveMode}

import edu.ucr.dblab.pflock.MF_Utils._
import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.sedona.quadtree.Quadtree

object PFlock {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params = new BFEParams(args)

    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .master(params.master())
      .appName("PFlock").getOrCreate()
    import spark.implicits._

    implicit var S = Settings(
      dataset = params.dataset(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = "PFlock",
      capacity = params.capacity(),
      fraction = params.fraction(),
      tolerance = params.tolerance(),
      tag = params.tag(),
      debug = params.debug(),
      output = params.output(),
      appId = spark.sparkContext.applicationId
    )

    implicit val G = new GeometryFactory(new PrecisionModel(S.scale))

    printParams(args)
    log(s"START|")

    /*******************************************************************************/
    // Code here...

    val trajs = spark.read
      .option("header", false)
      .option("delimiter", "\t")
      .csv(S.dataset)
      .rdd
      .mapPartitions{ rows =>
        val reader = new WKTReader(G)
        rows.map{ row =>
          val oid = row.getString(0).toInt
          val lon = row.getString(1).toDouble
          val lat = row.getString(2).toDouble
          val tid = row.getString(3).toInt

          val point = G.createPoint(new Coordinate(lon, lat))
          point.setUserData(Data(oid, tid))

          (tid, STPoint(point))
        }
      }.groupByKey().sortByKey().collect.toList

    @tailrec
    def join(trajs: List[(Int, Iterable[STPoint])], flocks: List[Disk]): List[Disk] = {

      trajs match {
        case current_trajs :: remaining_trajs =>
          val time = current_trajs._1
          val points = current_trajs._2.toList

          val (new_flocks, stats) = PSI.run(points)

          println(s"Processing time: $time")
          println(s"Number of Maximals disks: ${new_flocks.size}")
          debug{
            new_flocks.foreach{println}
          }

          join(remaining_trajs, flocks ++ new_flocks)

        case Nil => flocks
      }
    }

    val flocks = join(trajs, List.empty[Disk])

    log("Done!")
    log(s"Number of flocks:\t${flocks.size}")

    /*******************************************************************************/

    spark.close()

    log(s"END|")
  }
}
