package edu.ucr.dblab.djoin

import org.slf4j.{LoggerFactory, Logger}
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate}
import com.vividsolutions.jts.geom.{Point, Polygon, MultiPolygon}
import java.io.PrintWriter
import DisksFinder.calculateCenterCoordinates

object Demo {
  def save(filename: String)(content: Seq[String]): Unit = {
    val f = new PrintWriter(filename)
    f.write(content.mkString(""))
    f.close
    println(s"Saved $filename [${content.size} records].")
  }

  def main(args: Array[String]): Unit = {
    case class ST_Point(id: Int, point: Point)
    val model = new PrecisionModel(1000)
    val geofactory = new GeometryFactory(model)
    val p1 = ST_Point(1, geofactory.createPoint(new Coordinate(1, 1)))
    val p2 = ST_Point(2, geofactory.createPoint(new Coordinate(2, 8)))
    val p3 = ST_Point(3, geofactory.createPoint(new Coordinate(6, 5)))

    val points = List(p1,p2,p3)

    points.map(_.point).foreach{println}

    val epsilon = 10
    val pairs = for{
      a <- points
      b <- points if (a.id < b.id) && (a.point.distance(b.point) <= epsilon)
    } yield {
      (a.point, b.point)
    }

    pairs.foreach{println}

    val r2 = math.pow(epsilon / 2.0, 2)
    val centers = pairs.flatMap{ case(a, b) =>
      calculateCenterCoordinates(a, b, r2)
    }

    centers.foreach{println}
    save{"/tmp/test.tsv"}{
      centers.map{c => s"${c.toText}\n"}
    }
  }
}
