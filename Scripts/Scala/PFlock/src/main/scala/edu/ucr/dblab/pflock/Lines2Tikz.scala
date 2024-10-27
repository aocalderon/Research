package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.Utils.{round, save}
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, LineString, Point, PrecisionModel}
import org.locationtech.jts.io.WKTReader

import scala.io.Source

object Lines2Tikz {
  case class TikzLine(line: LineString, style: String = "solid", units: String = "mm", color: String = "black"){
    override val toString: String = line.toText
    def options: String = List(style, s"color=${color}").mkString(",")

    def coords: String = line.getCoordinates.map{ coord =>
      s"(${round(coord.x, 3)}, ${round(coord.y, 3)})"
    }.mkString(" -- ")

    def minus(x: Double, y: Double)(implicit G: GeometryFactory): TikzLine = {
      val coords = line.getCoordinates.map{ coord =>
        new Coordinate(coord.x - x, coord.y - y)
      }
      this.copy(line = G.createLineString(coords))
    }

    val tex: String = s"\\draw[${options}] $coords;\n"
  }

  def run(path: String, filename: String, output: String, minX: Double = 0.0, minY: Double = 0.0)(implicit G: GeometryFactory): Unit = {
    val reader = new WKTReader(G)
    val buffer = Source.fromFile(s"$path/$filename")
    val lines = buffer.getLines().map{ line =>
      val arr = line.split("\t")
      val linestring = reader.read(arr(0)).asInstanceOf[LineString]
      TikzLine(linestring, style = "dotted", color = "green")
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