package edu.ucr.dblab.pflock

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import com.vividsolutions.jts.geom.{Coordinate, Point}

import org.slf4j.Logger

import scala.io.Source
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import edu.ucr.dblab.pflock.pbk.PBK.bk
import edu.ucr.dblab.pflock.pbk.PBK_Utils.getEdges
import edu.ucr.dblab.pflock.welzl.Welzl

import MF_CMBC_EACH.{Timestamp, Now, loginfo, stamp, collectStats}

import Utils._

object MF_CMBC_COLLECT {
  // Helper function and case classes to log data...

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
    // COLLECT variant...
    now.t = System.currentTimeMillis
    // Collecting list of points in cliques not enclosed by unique MBC...
    val collect_list = outs.map{ case(mbc, points) =>
      points.map{ point =>
        val id = point.getUserData.asInstanceOf[Data].id
        (id, point)
      }
    }
    stamp("TIME2|collect")
    // Prunning duplicate points...
    val collect_set = collect_list.flatten.foldLeft(Map.empty[Long, Point]){
      (map, tuple) => map + tuple
    }.values.toList
    stamp("TIME2|duplicates")
    loginfo(s"INFO2|collect_set=${collect_set.size}")
    // Applying BFE over just that set...
    val pairs = computePairs(collect_set, settings.epsilon)
    stamp("TIME2|pairs")
    loginfo(s"INFO2|npairs=${pairs.length}")
    val centers = computeCenters(pairs)
    stamp("TIME2|centers")
    loginfo(s"INFO2|ncenters=${centers.length}")
    val disks = getDisks(collect_set, centers).map{ p =>
      val pids = p.getUserData.asInstanceOf[List[Long]]
      Disk(p, pids, List.empty[Int])
    }
    stamp("TIME2|disks")
    loginfo(s"INFO2|ndisks=${disks.length}")
    val maximals2 = pruneDisks(disks)
    stamp("TIME2|maximals")
    loginfo(s"INFO2|nmaximals=${maximals2.length}")    
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
}
