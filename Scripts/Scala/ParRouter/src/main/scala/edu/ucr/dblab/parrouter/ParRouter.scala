package edu.ucr.dblab.parrouter

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Coordinate, LineString}

import com.graphhopper.{GraphHopper, ResponsePath, GHRequest, GHResponse}
import com.graphhopper.config.{CHProfile,Profile}
import com.github.nscala_time.time.Imports._
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS

import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop._

import java.nio.file.{Files, Paths, StandardCopyOption}
import java.io.{BufferedWriter, FileWriter, File}
import scala.collection.JavaConverters._
import scala.io.Source

import ParResampler._

object ParRouter {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class RoutePoint(id: String, lon: Double, lat: Double, dateStr: String, zone: String = "UTC")
      extends Coordinate(lon, lat){

    val JAN_1_2000 = new LocalDateTime(2000, 1, 1, 0, 0).toDateTime

    val date: DateTime = if(dateStr == "")
      JAN_1_2000
    else
      new DateTime(dateStr.trim.replace(" ", "T"))

    val inMs: Long = date.toLocalDateTime().toDateTime(DateTimeZone.UTC).getMillis()
  }

  case class Route(id: String, line: LineString, t1: Long, t2: Long = -1L,
    duration: Long = 0){

    def resampleByTime(resample_rate: Int)(implicit G: GeometryFactory): Route = {
      val time = if(t2 == -1){ duration } else { t2 - t1 }
      val distance = line.getLength
      val speed = distance / time

      resampleByDistance(speed * resample_rate)
    }

    def resampleByDistance(resample_rate: Double)(implicit G: GeometryFactory): Route = {
      val resampled_line = resample(line, resample_rate)

      this.copy(line = resampled_line)
    }

    val wkt: String = s"${line.toText}\t$id\t$t1\t$t2\t$duration"

    override def toString: String = {
      val coor = line.getCoordinates
      val head = coor.head
      val last = coor.last
      val  dt1 = new DateTime(t1, DateTimeZone.UTC)
      val  dt2 = new DateTime(t2, DateTimeZone.UTC)

      s"$id,$dt1,$dt2,${head.x},${head.y},${last.x},${last.y}"
    }
  }

  def createGraphHopperInstance(ghLoc: String)(implicit P: ParRouterParams): GraphHopper = {
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
    (implicit GH: GraphHopper, G: GeometryFactory, P: ParRouterParams): Route = {

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
      if(P.debug()) println(s"$route")

      Route(p1.id, line = route, t1 = p1.inMs, t2 = p2.inMs, duration = timeInMs)
    } catch {
      case e: Throwable => {
        if(P.debug()) println("Error: " + e.toString)
        Route("", G.createLineString(), -1)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    implicit val params = new ParRouterParams(args)
    implicit val geofactory = new GeometryFactory(new PrecisionModel(1.0/params.tolerance()))

    logger.info("Reading...")
    val dataset_prime = if(params.dataset() == "") {
      val name = "sample_trajs.csv"
      val stream = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(name))
      val prime = stream.getLines
      val header = prime.next
      println(header.replaceAll(",", "\t"))
      val dataset_prime = prime.toList
      stream.close

      dataset_prime
    } else {
      val buffer = Source.fromFile(params.dataset())
      val dataset_prime = if(params.noheader()){
        buffer.getLines.toList
      } else {
        val prime = buffer.getLines
        val header = prime.next
        println(header.replaceAll(",", "\t"))
        prime.toList
      }
      buffer.close

      dataset_prime
    }

    val t1 = params.tim1_pos()
    val t2 = params.tim2_pos()
    val n = (t1, t2) match {
      case _ if t1 <  0 && t2 <  0 => 5
      case _ if t1 >= 0 && t2 <  0 => 6
      case _ if t1 <  0 && t2 >= 0 => 6
      case _ if t1 >= 0 && t2 >= 0 => 7
    }
    val dataset = dataset_prime.par
      .filter(_.split(",").size == n)
      .map{ line =>
        val arr = line.split(",")

        val id = arr(params.id_pos())

        val tim1 = if(params.tim1_pos() < 0) "" else arr(params.tim1_pos())
        val lon1 = arr(params.lon1_pos()).toDouble
        val lat1 = arr(params.lat1_pos()).toDouble
        val   p1 = RoutePoint(id, lon1, lat1, tim1)

        val tim2 = if(params.tim2_pos() < 0) "" else arr(params.tim2_pos())
        val lon2 = arr(params.lon2_pos()).toDouble
        val lat2 = arr(params.lat2_pos()).toDouble
        val   p2 = RoutePoint(id, lon2, lat2, tim2)

        (p1, p2)
      }

    import java.io.{DataOutputStream, FileOutputStream}
    implicit var progress = new pb.ProgressBar(dataset.size)
    val f = new BufferedWriter(new FileWriter(params.output()), 16384) // Buffer size...
    logger.info("Routing...")
    val osmLocation = if(params.osm() == ""){
      val osmLocation = "/tmp/sample.osm"
      val name = "sample.pbf"
      val stream = getClass.getClassLoader.getResourceAsStream(name)
      Files.copy(stream, Paths.get(osmLocation), StandardCopyOption.REPLACE_EXISTING)
      stream.close

      osmLocation
    } else {
      params.osm()
    }
    implicit val hopper = createGraphHopperInstance(osmLocation)
    dataset.foreach{ case(p1, p2) =>
      progress.add(1)
      val route = if(params.resample())
          routing(p1, p2).resampleByTime(params.resample_rate())
        else
          routing(p1, p2)
      if(route.id != "") f.write(s"${route.wkt}\n")
    }
    progress.current = dataset.size - 1
    progress.add(1)
   
    logger.info("Closing...")
    hopper.close
    f.close
  }
}

class ParRouterParams(args: Seq[String]) extends ScallopConf(args) {
  val path = new File(".").getCanonicalPath
  println(s"Working directory: $path")

  val tolerance:     ScallopOption[Double]  = opt[Double]  (default = Some(1e-6))
  val dataset:       ScallopOption[String]  = opt[String]  (default = Some(""))
  val delimiter:     ScallopOption[String]  = opt[String]  (default = Some("\t"))
  val id_pos:        ScallopOption[Int]     = opt[Int]     (default = Some(0))
  val tim1_pos:      ScallopOption[Int]     = opt[Int]     (default = Some(1))
  val tim2_pos:      ScallopOption[Int]     = opt[Int]     (default = Some(2))
  val lon1_pos:      ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val lat1_pos:      ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val lon2_pos:      ScallopOption[Int]     = opt[Int]     (default = Some(6))
  val lat2_pos:      ScallopOption[Int]     = opt[Int]     (default = Some(5))
  val osm:           ScallopOption[String]  = opt[String]  (default = Some(""))
  val profile:       ScallopOption[String]  = opt[String]  (default = Some("bike"))
  val resample:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val resample_rate: ScallopOption[Int]     = opt[Int]     (default = Some(5000))
  val output:        ScallopOption[String]  = opt[String]  (default = Some("/tmp/edgesT.wkt"))
  val noheader:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:         ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
