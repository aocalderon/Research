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

import Utils._

object Tester {
  case class Timestamp(t: Long)

  def loginfo(msg: String)(implicit start: Timestamp, logger: Logger, settings: Settings): Unit = {
    val now = System.currentTimeMillis
    val e = settings.epsilon.toInt
    val m = settings.mu
    val a = settings.appId
    logger.info(f"|LOG|$a|$e|$m|${start.t}|${now}|${now - start.t}%5s|${msg}")
  }

  def main(args: Array[String]): Unit = {
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

    loginfo(s"INFO|Start")
    val input = params.input()

    val pointsBuffer = Source.fromFile(input)
    val points = pointsBuffer.getLines.map{ line =>
      val arr = line.split("\t")
      val id = arr(0).toInt
      val x = arr(1).toDouble
      val y = arr(2).toDouble
      val point = geofactory.createPoint(new Coordinate(x, y))
      val t = arr(3).toInt
      point.setUserData(Data(id, t))
      point
    }.toList
    pointsBuffer.close
    
    loginfo(s"INFO|points=${points.size}")
    val vertices = points
    val edges = getEdges(vertices, settings.epsilon)
    loginfo(s"TIME|Graph")
    val cliques = bk(vertices, edges).iterator
      .filter(_.size >= settings.mu) // Filter cliques with no enough points...
    loginfo(s"TIME|Cliques")
    val (maximals_prime, disks_prime) = cliques.map{ points =>
      val mbc = Welzl.mbc(points)
      (mbc, points)
    }.partition{ case (mbc, points) => // Split cliques enclosed by a single disk...
        round(mbc.getRadius) < settings.r
    }
    loginfo(s"TIME|MBC")
    val maximals1 = maximals_prime.map{ case(mbc, points) =>
      val pids = points.map(_.getUserData.asInstanceOf[Data].id)
      val center = geofactory.createPoint(new Coordinate(mbc.getCenter.getX,
        mbc.getCenter.getY))

      Disk(center, pids, List.empty)
    }.toList
    loginfo("TIME|EnclosedByMBC")
    val maximals2 = disks_prime.map{ case(mbc, points) =>
      val centers = computeCenters(computePairs(points, settings.epsilon))
      val disks = getDisks(points, centers).map{ p =>
        val pids = p.getUserData.asInstanceOf[List[Long]]
        Disk(p, pids, List.empty[Int])
      }
      pruneDisks(disks)
    }.toList.flatten
    loginfo("TIME|NotEnclosedByMBC")
    val maximals = pruneDisks(maximals1 ++ maximals2)
    loginfo("TIME|Prune")
    loginfo(s"INFO|flocks=${maximals.size}")

    if(settings.debug){
      save("/tmp/flocks.txt"){
        maximals.map{ maximal =>
          maximal.toString + "\n"
        }
      }
    }
    loginfo("INFO|End")
  }
}
