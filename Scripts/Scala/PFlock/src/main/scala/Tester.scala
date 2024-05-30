package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.MF_Utils._
import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.sedona.quadtree.{QuadRectangle, StandardQuadTree}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom._
import org.locationtech.jts.index.quadtree.{Quadtree => JTSQuadtree}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.xml._

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
    val base = spark.read
      .option("header", value = false)
      .option("delimiter", "\t")
      .csv(S.dataset)
      .rdd
      .mapPartitions { rows =>
        rows.map { row =>
          val oid = row.getString(0).toInt
          val lon = row.getString(1).toDouble
          val lat = row.getString(2).toDouble
          val tid = row.getString(3).toInt

          (oid, lon, lat, tid)

        }
      }.cache

    val trajs = base.filter{ p => p._4 == S.endtime }.cache

    val nTrajs = trajs.count()
    log(s"Number of trajectories: $nTrajs")

    save("/tmp/edgesLA.wkt"){
      trajs.map{ case(o,x,y,t) =>
        val wkt = G.createPoint(new Coordinate(x,y)).toText
        s"$wkt\t$o\t$t\n"
      }.collect()
    }

    val center = G.createPoint(new Coordinate(1982002.286, 561972.289))
    val env = center.getEnvelopeInternal
    env.expandBy(255)

    val trajs2 = trajs.map{ case(o,x,y,t) =>
      val p = G.createPoint(new Coordinate(x,y))
      p.setUserData(o)
      (o, p)
    }.filter{ case (o, p) => env.contains(p.getEnvelopeInternal) }

    val trajs3 = trajs2.map{ case(o, p) =>
        val wkt = p.toText
        s"$wkt\t$o\n"
    }
    save("/tmp/edgesS.wkt"){
      trajs3.collect
    }

    val ids = trajs2.map{ case(o, p) => o }.sample(withReplacement = false, fraction = 0.85, seed = 42).collect.toSet
    println(ids.size)

    val base_prime = base.map{ case(o,x,y,t) =>
      if(!ids.contains(o))
        s"$o\t$x\t$y\t$t"
      else
        ""
    }.filter(_ != "").cache
    println(base_prime.count())

    base_prime.toDF().repartition(1024).write.text("PFlock/LA_50K")

    /*******************************************************************************/

    spark.close()

    log(s"END|")
  }
}
