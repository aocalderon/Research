package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.Utils.{round, save}
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Polygon, PrecisionModel}
import org.locationtech.jts.io.WKTReader

import scala.io.Source

object Polygons2Tikz {
  case class TikzPolygon(polygon: Polygon, style: String = "solid", fill: String = "black"){
    override val toString: String = polygon.toText
    def options: String = List(style, s"fill=${fill}").mkString(",")

    def coords: String = polygon.getCoordinates.map{ coord =>
      s"(${round(coord.x, 3)}, ${round(coord.y, 3)})"
    }.mkString(" -- ")

    def minus(x: Double, y: Double)(implicit G: GeometryFactory): TikzPolygon = {
      val coords = polygon.getCoordinates.map{ coord =>
        new Coordinate(coord.x - x, coord.y - y)
      }
      this.copy(polygon = G.createPolygon(coords))
    }

    val tex: String = s"\\draw[${options}] $coords -- cycle;\n"
  }

  def run(path: String, filename: String, output: String, minX: Double = 0.0, minY: Double = 0.0)(implicit G: GeometryFactory): Unit = {
    val reader = new WKTReader(G)
    val buffer = Source.fromFile(s"$path/$filename")
    val lines = buffer.getLines().map{ line =>
      val arr = line.split("\t")
      val polygon = reader.read(arr(0)).asInstanceOf[Polygon]
      TikzPolygon(polygon, style = "solid", fill = "brown!10")
    }.toList
    buffer.close()

    save(s"$path/$output"){
      lines.map{ line =>
        val tex = line.minus(minX, minY).tex
        println(tex)
        tex
      }
    }
  }
  def main(args: Array[String]): Unit = {
    implicit val P: TikzParams = new TikzParams(args)
    implicit val G: GeometryFactory = new GeometryFactory(new PrecisionModel(1e3))

    run(P.path(), P.filename(), P.output())
  }
}