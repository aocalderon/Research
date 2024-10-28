package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.Utils.{round, save}
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, LineString, Point, PrecisionModel}
import org.locationtech.jts.io.WKTReader

import scala.collection.mutable.ListBuffer
import scala.io.Source

object Lines2Tikz {
  case class TikzLine(line: LineString){
    override val toString: String = line.toText

    def coords: String = line.getCoordinates.map{ coord =>
      s"(${round(coord.x/10.0, 2)}, ${round(coord.y/10.0, 2)})"
    }.mkString(" -- ")

    def minus(x: Double, y: Double)(implicit G: GeometryFactory): TikzLine = {
      val coords = line.getCoordinates.map{ coord =>
        new Coordinate(coord.x - x, coord.y - y)
      }
      this.copy(line = G.createLineString(coords))
    }

    val tex: String = s"\\draw[line] $coords;\n"
  }

  def run(scale: String = "1", color: String = "red", style: String = "solid")
         (implicit P: TikzParams, G: GeometryFactory): Unit = {

    val name = P.filename().split("\\.")(0)
    val main_file = new ListBuffer[String]()
    main_file.append("\\documentclass[border=1]{standalone}\n")
    main_file.append("\\usepackage{tikz}\n")
    main_file.append("\n")
    main_file.append("\\tikzset{\n")
    main_file.append(s"    line/.style={ draw, $style, scale=$scale, color=$color }\n")
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
      val linestring = reader.read(arr(0)).asInstanceOf[LineString]
      TikzLine(linestring)
    }.toList
    buffer.close()

    save(s"${P.path()}/${name}_prime.tex"){
      lines.map{ line =>
        val tex = line.tex
        println(tex)
        tex
      }
    }
  }
  def main(args: Array[String]): Unit = {
    implicit val P: TikzParams = new TikzParams(args)
    implicit val G: GeometryFactory = new GeometryFactory(new PrecisionModel(1e3))

    run()
  }
}