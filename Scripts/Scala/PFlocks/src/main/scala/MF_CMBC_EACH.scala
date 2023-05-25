package edu.ucr.dblab.pflock

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Coordinate, Point}

import org.slf4j.Logger

import scala.io.Source
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.commons.geometry.enclosing.EnclosingBall
import org.apache.commons.geometry.euclidean.twod.Vector2D

import edu.ucr.dblab.pflock.pbk.PBK.bk
import edu.ucr.dblab.pflock.pbk.PBK_Utils.getEdges
import edu.ucr.dblab.pflock.welzl.Welzl

import Utils._

object MF_CMBC_EACH {
  // Helper function and case classes to log data...
  case class Timestamp(t: Long)
  case class Now(var t: Long)

  def loginfo(msg: String)
    (implicit start: Timestamp, logger: Logger, settings: Settings): Unit = {
    val now = System.currentTimeMillis
    val e = settings.epsilon.toInt
    val m = settings.mu
    val a = settings.appId
    logger.info(f"$a|$e|$m|${start.t}|${now}|${now - start.t}|${msg}")
  }

  def stamp(msg: String)
    (implicit start: Timestamp, mark: Now, logger: Logger, settings: Settings): Unit = {
    val now = System.currentTimeMillis
    val e = settings.epsilon.toInt
    val m = settings.mu
    val a = settings.appId
    logger.info(f"$a|$e|$m|${start.t}|${now}|${now - mark.t}|${msg}")
    mark.t = now
  }

  def main(args: Array[String]): Unit = {
    // Starting session...
    implicit var now = Now(System.currentTimeMillis())
    implicit val params = new Params(args)
    implicit val settings = Settings(
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      tolerance = params.tolerance(),
      debug = params.debug(),
      appId = params.tag()
    )
    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))
    implicit val start = Timestamp(System.currentTimeMillis())
    val properties = System.getProperties().asScala
    loginfo(s"COMMAND|${properties("sun.java.command")}")
    loginfo(s"INFO|Start")

    // Reading input data...
    val input = params.input()
    val pointsBuffer = Source.fromFile(input)
    val points = pointsBuffer.getLines.map{ line =>
    val arr = line.split("\t")
      val id = arr(0).toLong
      val x = arr(1).toDouble
      val y = arr(2).toDouble
      val point = geofactory.createPoint(new Coordinate(x, y))
      val t = arr(3).toInt
      point.setUserData(Data(id, t))
      point
    }.toList
    pointsBuffer.close
    loginfo(s"INFO|points=${points.size}")
    loginfo(s"TIME|Read")

    // Building initial graph...
    val vertices = points
    val edges = getEdges(vertices, settings.epsilon)
    loginfo(s"TIME|Graph")

    // Computing cliques...
    val cliques = bk(vertices, edges).iterator
      .filter(_.size >= settings.mu) // Filter cliques with no enough points...
    loginfo(s"TIME|Cliques")

    // Getting cliques enclosed by an unique MBC (maximals_prime) and
    // those which require further analysis (disks_prime)...
    val (maximals_prime, disks_prime) = cliques.map{ points =>
      val mbc = Welzl.mbc(points)
      (mbc, points)
    }.partition{ case (mbc, points) => // Split cliques enclosed by a single disk...
      round(mbc.getRadius) < settings.r
    }
    loginfo(s"TIME|MBC")

    // Collecting stats...
    val ins  = maximals_prime.toList
    val outs = disks_prime.toList
    collectStats(ins, outs)
    loginfo(s"TIME|Stats")

    // Collecting flocks on cliques enclosed by unique MBC...
    val maximals1 = ins.map{ case(mbc, points) =>
      val pids = points.map(_.getUserData.asInstanceOf[Data].id)
      val center = geofactory.createPoint(new Coordinate(mbc.getCenter.getX,
        mbc.getCenter.getY))

      Disk(center, pids, List.empty)
    }.toList
    loginfo("TIME|EnclosedByMBC")

    // Collecting flocks on cliques not enclosed by unique MBC...
    // EACH variant...
    val maximals2 = outs.map{ case(mbc, points) =>
      loginfo(s"INFO2|n=${points.length}")
      now = Now(System.currentTimeMillis)
      val pairs = computePairs(points, settings.epsilon)      
      stamp("TIME2|pairs")
      loginfo(s"INFO2|npairs=${pairs.length}")
      val centers = computeCenters(pairs)
      stamp("TIME2|centers")
      loginfo(s"INFO2|ncenters=${centers.length}")
      val disks = getDisks(points, centers).map{ p =>
        val pids = p.getUserData.asInstanceOf[List[Long]]
        Disk(p, pids, List.empty[Int])
      }
      stamp("TIME2|disks")
      loginfo(s"INFO2|ndisks=${disks.length}")      
      val maximals = pruneDisks(disks)
      stamp("TIME2|prune")
      loginfo(s"INFO2|nmaximals=${maximals.length}")      
      maximals
    }.toList.flatten
    loginfo("TIME|NotEnclosedByMBC")

    // Prunning possible duplicates...
    val maximals = pruneDisks(maximals1 ++ maximals2)
    loginfo("TIME|Prune")

    // Reporting total number of flocks...
    loginfo(s"INFO|flocks=${maximals.size}")

    // Ending session...
    if(settings.debug){
      save(s"/tmp/flocks${params.tag()}.txt"){
        maximals.map{ maximal =>
          maximal.pids.mkString(" ") + "\n"
        }
      }
    }
    loginfo("INFO|End")    
  }

  def collectStats(ins: List[(EnclosingBall[Vector2D], List[Point])],
    outs: List[(EnclosingBall[Vector2D], List[Point])])
    (implicit start: Timestamp, logger: Logger, settings: Settings): Unit = {

    val ncliques1 = ins.size
    val points_prime1 = ins.flatMap(_._2).toList
    val replicatedPoints1 = points_prime1.size
    val uniquePoints1 = points_prime1.map(_.getUserData.asInstanceOf[Data].id).distinct.size
    val npoints1 = ins.map(_._2.length)
    val max1 = npoints1.max
    val min1 = npoints1.min
    val avg1 = _round(mean(npoints1))
    val sd1  = _round(stdDev(npoints1))
    loginfo("INFO|mode=1;" +
      s"min=$min1;max=$max1;avg=$avg1;sd=$sd1;" +
      s"ncliques=${ncliques1};" +
      s"uniquePoints=${uniquePoints1};" +
      s"replicatedPoints=${replicatedPoints1}"
    )
    val ncliques2 = outs.size
    val points_prime2 = outs.flatMap(_._2).toList
    val replicatedPoints2 = points_prime2.size
    val uniquePoints2 = points_prime2.map(_.getUserData.asInstanceOf[Data].id).distinct.size
    val npoints2 = outs.map(_._2.length)
    val max2 = npoints2.max
    val min2 = npoints2.min
    val avg2 = _round(mean(npoints2))
    val sd2  = _round(stdDev(npoints2))
    loginfo("INFO|mode=2;" +
      s"min=$min2;max=$max2;avg=$avg2;sd=$sd2;" +
      s"ncliques=${ncliques2};" +
      s"uniquePoints=${uniquePoints2};" +
      s"replicatedPoints=${replicatedPoints2}"
    )
  }  
}
