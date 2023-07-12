package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point, LineString}
import org.locationtech.jts.index.quadtree.{Quadtree => JTSQuadtree}

import com.graphhopper.{GraphHopper, ResponsePath, GHRequest, GHResponse}
import com.graphhopper.config.{CHProfile,Profile}

import org.slf4j.{Logger, LoggerFactory}

import scala.xml._
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
      output = params.output(),
      appId = spark.sparkContext.applicationId
    )

    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))

    printParams(args)
    log(s"START|")

    def createGraphHopperInstance(ghLoc: String): GraphHopper = {
      val hopper = new GraphHopper()
      hopper.setOSMFile(ghLoc)
      // specify where to store graphhopper files
      hopper.setGraphHopperLocation("target/routing-graph-cache")

      // see docs/core/profiles.md to learn more about profiles
      hopper.setProfiles(new Profile("bike")
        .setVehicle("bike")
        .setWeighting("fastest")
        .setTurnCosts(false)
      )

      // this enables speed mode for the profile we called bike
      hopper.getCHPreparationHandler()
        .setCHProfiles(new CHProfile("bike"))

      // now this can take minutes if it imports or a few seconds for loading
      // of course this is dependent on the area you import
      hopper.importOrLoad()

      hopper
    }

    def routing(hopper: GraphHopper,
      x1:Double, y1: Double, x2:Double, y2: Double): (LineString, Long) = {

      // simple configuration of the request object, note that we have to specify which
      //   profile we are using even when there is only one like here...
      val req = new GHRequest(x1, y1, x2, y2).setProfile("bike")
      val rsp = hopper.route(req)

      // handle errors
      if ( rsp.hasErrors ){   // should 
        (null, 0)  // throw new RuntimeException(rsp.getErrors().toString())...
      } else {
        // use the best path, see the GHResponse class for more possibilities...
        val path: ResponsePath  = rsp.getBest()

        // points, distance in meters and time in millis of the full path...
        val timeInMs: Long = path.getTime()
        val coords = path.getPoints().asScala.toArray.map{ ghpoint =>
          new Coordinate(ghpoint.getLon(), ghpoint.getLat())
        }
        val route = geofactory.createLineString(coords)

        (route, timeInMs)
      }
    }
    
    val trips = spark.read
      .textFile(settings.dataset).rdd
      .filter(_.split(",").size == 5)
      .mapPartitions { it =>
        it.map{ line =>
          val arr = line.split(",")
          val x1  = arr(2).toDouble
          val y1  = arr(1).toDouble
          val x2  = arr(4).toDouble
          val y2  = arr(3).toDouble
          val osmFile = "/home/acald013/opt/sumo/sumo_test/ny/ny.osm"
          val  hopper = createGraphHopperInstance(osmFile)
          val (route, timeInMs) = routing(hopper, x1, y1, x2, y2)
          hopper.close
          (route, timeInMs)
        }.filter(_._2 > 0)
      }
    val t = trips.map{ case(r, t) => s"${r.toText}\t$t\n" }.collect.mkString("")

    val f = new java.io.FileWriter(params.output())
    f.write(s"$t")
    f.close

    spark.close()

    log(s"END|")
  }
}
