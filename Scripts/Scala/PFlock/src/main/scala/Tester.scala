package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.MF_Utils._
import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.sedona.quadtree.{QuadRectangle, StandardQuadTree}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom
import org.locationtech.jts.geom._
import org.locationtech.jts.index.quadtree.{Quadtree => JTSQuadtree}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.io.Source

object Tester {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params = new BFEParams(args)

    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .master(params.master())
      .appName("Tester").getOrCreate()
    import spark.implicits._

    implicit val S = Settings(
      dataset = params.dataset(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = params.method(),
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
    import java.nio.file.{FileSystems, Files}
    val path = "/home/and/Datasets/PFlocks/cells/"
    val dir = FileSystems.getDefault.getPath(path)
    val grids = Files.list(dir).iterator().asScala.filter(Files.isRegularFile(_)).map{ file =>
      val filename = s"$path/${file.getFileName.toString}"
      val i = file.getFileName.toString.split("\\.")(0).split("cell")(1)

      val buffer = Source.fromFile(filename)
      val points = buffer.getLines().map{ line =>
        val arr = line.split("\\t")
        val x = arr(1).toDouble
        val y = arr(2).toDouble
        G.createPoint(new Coordinate(x, y))
      }.toArray
      val envelope = G.createMultiPoint(points).getEnvelopeInternal
      val area = envelope.getArea
      val n = points.length.toDouble
      val d = n / area
      val wkt = G.toGeometry(envelope).toText
      buffer.close()

      s"$wkt\t$i\t$n\t$area\t$d\n"
    }

    save("/tmp/edgesL.wkt"){
      grids.toList
    }

    /*******************************************************************************/

    spark.close()

    log(s"END|")
  }
}
