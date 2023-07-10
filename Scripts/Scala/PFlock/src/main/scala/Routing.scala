package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Coordinate, LineString}

import com.graphhopper.{GraphHopper, ResponsePath, GHRequest, GHResponse}
import com.graphhopper.config.{CHProfile,Profile}
import com.github.nscala_time.time.Imports._

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.io.Source
import java.io.{BufferedWriter, FileWriter}


object Routing {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class RoutePoint(id: String, lon: Double, lat: Double, dateStr: String, zone: String = "UTC")
      extends Coordinate(lon, lat){

    val JAN_1_2000 = new LocalDateTime(2000, 1, 1, 0, 0).toDateTime

    val date: DateTime = if(dateStr == "")
      JAN_1_2000
    else
      new DateTime(dateStr.replace(" ", "T"))

    val inMs: Long = date.toLocalDateTime().toDateTime(DateTimeZone.UTC).getMillis()
  }

  case class Route(id: String, line: LineString, t1: Long, t2: Long = -1L,
    duration: Long = 0){

    val wkt: String = s"${line.toText}\t$id\t$t1\t$t2\t$duration"
  }

  def createGraphHopperInstance(ghLoc: String)(implicit P: RoutingParams): GraphHopper = {
    val hopper = new GraphHopper()
    hopper.setOSMFile(ghLoc)
    // specify where to store graphhopper files
    hopper.setGraphHopperLocation("target/routing-graph-cache")

    // see docs/core/profiles.md to learn more about profiles
    hopper.setProfiles(new Profile(P.profile())
      .setVehicle(P.profile())
      .setWeighting("fastest")
      .setTurnCosts(false)
    )

    // this enables speed mode for the profile we called bike
    hopper.getCHPreparationHandler()
      .setCHProfiles(new CHProfile(P.profile()))

    // now this can take minutes if it imports or a few seconds for loading
    // of course this is dependent on the area you import
    hopper.importOrLoad()

    hopper
  }

  def routing(p1: RoutePoint, p2: RoutePoint)
    (implicit GH: GraphHopper, G: GeometryFactory, P: RoutingParams): Route = {

    if(P.debug()) println(p1)
    if(P.debug()) println(p2)

    // simple configuration of the request object, note that we have to specify which
    //   profile we are using even when there is only one like here...
    val req = new GHRequest(p1.lon, p1.lat, p2.lon, p2.lat).setProfile(P.profile())
    val rsp = GH.route(req)

    // handle errors
    try{
      if(rsp.hasErrors) scala.util.control.Exception
      // use the best path, see the GHResponse class for more possibilities...
      val path: ResponsePath  = rsp.getBest()

      // points, distance in meters and time in millis of the full path...
      val timeInMs: Long = path.getTime()
      val coords = path.getPoints().asScala.toArray.map{ ghpoint =>
        new Coordinate(ghpoint.getLon(), ghpoint.getLat())
      }
      val route = G.createLineString(coords)
      if(P.debug()) println(route.toText)

      Route(p1.id, line = route, t1 = p1.inMs, t2 = p2.inMs, duration = timeInMs)
    } catch {
      case e: Throwable => {
        if(P.debug()) println("Error: " + e.toString)
        Route("", G.createLineString(), -1)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    implicit val params = new RoutingParams(args)
    implicit val geofactory = new GeometryFactory(new PrecisionModel(1.0/params.tolerance()))

    logger.info("Reading...")
    val buffer = Source.fromFile(params.dataset())
    val od_prime = if(params.header()){
      val prime = buffer.getLines
      val header = prime.next
      println(header.replaceAll(",", "\t"))
      prime.toList
    } else {
      buffer.getLines.toList
    }

    val t1 = params.tim1_pos()
    val t2 = params.tim2_pos()
    val n = (t1, t2) match {
      case _ if t1 <  0 && t2 <  0 => 5
      case _ if t1 >= 0 && t2 <  0 => 6
      case _ if t1 <  0 && t2 >= 0 => 6
      case _ if t1 >= 0 && t2 >= 0 => 7
    }
    val od = od_prime.par
      .filter(_.split(",").size == n)
      .map{ line =>
        val arr = line.split(",")

        val id = arr(params.id_pos())

        val tim1 = if(params.tim1_pos() < 0) "" else arr(params.tim1_pos())
        val lon1 = arr(params.lon1_pos()).toDouble
        val lat1 = arr(params.lat1_pos()).toDouble
        val   p1 = RoutePoint(id, lon1, lat1, tim1)
        if(params.debug()) print(p1)

        val tim2 = if(params.tim2_pos() < 0) "" else arr(params.tim2_pos())
        val lon2 = arr(params.lon2_pos()).toDouble
        val lat2 = arr(params.lat2_pos()).toDouble
        val   p2 = RoutePoint(id, lon2, lat2, tim2)
        if(params.debug()) print(p2)

        (p1, p2)
      }
    buffer.close

    implicit var progress = new pb.ProgressBar(od.size)
    val f = new BufferedWriter(new FileWriter(params.output()), 16384) // Buffer size...
    logger.info("Routing...")
    implicit val hopper = createGraphHopperInstance(params.osm())
    val trips = od
      .foreach{ case(p1, p2) =>
        progress.add(1)
        val route = routing(p1, p2)
        if(route.id != "") f.write(s"${route.wkt}\n")
      }

    logger.info("Closing...")
    hopper.close
    f.close
  }
}

import org.rogach.scallop._

class RoutingParams(args: Seq[String]) extends ScallopConf(args) {
  val default_dataset = "/home/acald013/opt/sumo/sumo_test/ny/sample.csv"
  val default_osm = "/home/acald013/opt/sumo/sumo_test/ny/ny.osm"

  val tolerance: ScallopOption[Double]  = opt[Double]  (default = Some(1e-3))
  val dataset:   ScallopOption[String]  = opt[String]  (default = Some(default_dataset))
  val id_pos:    ScallopOption[Int]     = opt[Int]     (default = Some(0))
  val tim1_pos:  ScallopOption[Int]     = opt[Int]     (default = Some(-1))
  val tim2_pos:  ScallopOption[Int]     = opt[Int]     (default = Some(-1))
  val lon1_pos:  ScallopOption[Int]     = opt[Int]     (default = Some(1))
  val lat1_pos:  ScallopOption[Int]     = opt[Int]     (default = Some(2))
  val lon2_pos:  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val lat2_pos:  ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val osm:       ScallopOption[String]  = opt[String]  (default = Some(default_osm))
  val profile:   ScallopOption[String]  = opt[String]  (default = Some("car"))
  val output:    ScallopOption[String]  = opt[String]  (default = Some("/tmp/edgesT.wkt"))
  val header:    ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:     ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
