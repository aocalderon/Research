package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.Utils.{round, save}
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Polygon, PrecisionModel}
import org.locationtech.jts.io.WKTReader

import scala.collection.mutable.ListBuffer
import scala.io.Source

object Polygons2Tikz {
  case class TikzPolygon(polygon: Polygon){
    override val toString: String = polygon.toText

    def coords: String = polygon.getCoordinates.map{ coord =>
      s"(${round(coord.x/10.0, 3)}, ${round(coord.y/10.0, 3)})"
    }.mkString(" -- ")

    def minus(x: Double, y: Double)(implicit G: GeometryFactory): TikzPolygon = {
      val coords = polygon.getCoordinates.map{ coord =>
        new Coordinate(coord.x - x, coord.y - y)
      }
      this.copy(polygon = G.createPolygon(coords))
    }

    val tex: String = s"\\draw[polygon] $coords -- cycle;\n"
  }

  def run(style: String = "solid", color: String = "black")(implicit P: TikzParams, G: GeometryFactory): Unit = {
    val name = P.filename().split("\\.")(0)
    val main_file = new ListBuffer[String]()
    main_file.append("\\documentclass[border=1]{standalone}\n")
    main_file.append("\\usepackage{tikz}\n")
    main_file.append("\n")
    main_file.append("\\tikzset{\n")
    main_file.append(s"\t polygon/.style={ draw, $style, color=$color }\n")
    main_file.append("}\n")
    main_file.append("\n")
    main_file.append("\\begin{document}\n")
    main_file.append("    \\begin{tikzpicture}\n")
    main_file.append(s"\\input{${name}_prime}\n")
    main_file.append("    \\end{tikzpicture}\n")
    main_file.append("\\end{document}\n")

    save(s"${P.path()}/${P.output()}") {
      main_file.toList
    }

    val reader = new WKTReader(G)
    val buffer = Source.fromFile(s"${P.path()}/${P.filename()}")
    val lines = buffer.getLines().map{ line =>
      val arr = line.split("\t")
      val polygon = reader.read(arr(0)).asInstanceOf[Polygon]
      TikzPolygon(polygon)
    }.toList
    buffer.close()

    save(s"${P.path()}/${name}_prime.tex"){
      lines.map{ line =>
        val tex = line.tex
        tex
      }
    }
  }
  def main(args: Array[String]): Unit = {
    implicit val P: TikzParams = new TikzParams(args)
    implicit val G: GeometryFactory = new GeometryFactory(new PrecisionModel(1e3))

    Polygons2Tikz.run()
  }
}