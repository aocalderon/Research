package edu.ucr.dblab;

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Coordinate, Geometry, Point, Polygon}
import org.jgrapht.graph.{SimpleGraph, DefaultEdge}
import scala.collection.JavaConverters._

object TestPBK {
  val model = new PrecisionModel(1000)
  val geofactory = new GeometryFactory(model)

  def main(args: Array[String]): Unit = {
    val graph = new SimpleGraph[P, DefaultEdge](classOf[DefaultEdge])

    val p1 = new P(1, 1, 5)
    val p2 = new P(2, 2, 4)
    val p3 = new P(3, 1, 3)
    val p4 = new P(4, 2, 2)
    val p5 = new P(5, 3, 1)
    val p6 = new P(6, 2, 0)
    val p7 = new P(7, 0, 0)
    val p8 = new P(8, 0, 2)
    val p9 = new P(9, 0, 4)

    val vertices = List(
      p1,p2,p3,p4,p5,p6,p7,p8,p9
    )
    val edges = List(
      (p1,p2),(p1,p9),
      (p2,p3),(p2,p9),
      (p3,p4),(p3,p8),(p3,p9),
      (p4,p5),(p4,p6),(p4,p7),(p4,p8),
      (p5,p6),
      (p6,p7),(p6,p8),
      (p7,p8)
    )

    vertices.foreach{ vertex =>  graph.addVertex(vertex) }
    edges.foreach{ case(a, b) => graph.addEdge(a, b) }

    val finder = new PBKCliqueFinder(graph)

    val r = finder.iterator.asScala.toList.zipWithIndex.map{ case(points, id) =>
      val clique = new Clique(points)

      s"${clique.wkt}\t${id}"
    }

    val logger = finder.getLogger.split("\n")
    logger.foreach{println}

    println
    println("Final report:")
    r.foreach{println}

    case class Log(iter: Int, step: Int, tag: String, value: String, order: Int){
      override def toString: String = s"$iter\t$step\t$tag\t$value\t$order"
    }
    val points = vertices.map(p => p.id -> p.point).toMap
    val logs = logger.zipWithIndex.map{ case(log, order) =>
      val arr = log.split("\\|")
      val iter = arr(0).toInt
      val step = arr(1).toInt
      val tag = arr(2)
      val value = arr(3)

      Log(iter, step, tag, value, order)
    }

    def getPointWKT(log: Log): List[String] = if(log.value == "{}"){
      val point = geofactory.createPoint(new Coordinate(-1,-1))
      point.setUserData(log)
      List(point.toText + "\t" + log.toString)
    } else {
      log.value.split(" ").map{i =>
        val pid = i.trim.toInt
        val point = points(pid)
        point.setUserData(log)
        point.toText + "\t" + log.toString
      }.toList
    }

    def getPolygonWKT(log: Log): List[String] = if(log.value == "{}"){
      val polygon = geofactory.createPolygon(Array.empty[Coordinate])
      polygon.setUserData(log)
      List(polygon.toText + "\t" + log.toString)
    } else {
      val coords = log.value.split(" ").map{i =>
        val pid = i.trim.toInt
        points(pid).getCoordinate
      }.toArray
      val polygon = geofactory.createMultiPoint(coords).convexHull.asInstanceOf[Polygon]
      polygon.setUserData(log)
      List(polygon.toText + "\t" + log.toString)
    }

    val wkts = logs
      //.filter(_.iter == 0)
      .flatMap{ log =>
        log.tag match {
          case "Clique" => getPolygonWKT(log)
          case _ => getPointWKT(log)
        }
      }

    def save(name: String, c: Seq[String]): Unit = {
      val f = new java.io.PrintWriter(name)
      f.write(c.mkString("\n"))
      f.close
      println(s"Saved $name [${c.size} records].")
    }
    val (wkt1, wkt2) = wkts.partition(_.contains("POLYGON"))
    save("/tmp/edgesP1.wkt", wkt1)
    save("/tmp/edgesP2.wkt", wkt2)
    save("/tmp/edgesV.wkt", vertices.map(v => v.wkt + "\t" + v.id))
    save("/tmp/edgesE.wkt", edges.map{ case(a, b) =>
      val coords = Array(a.point.getCoordinate, b.point.getCoordinate)
      val edge = geofactory.createLineString(coords)
      s"${edge.toText}\t(${a.id}, ${b.id})"
    })
  }
}
