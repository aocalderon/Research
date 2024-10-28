package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{GeometryFactory, Point, PrecisionModel}
import org.locationtech.jts.io.WKTReader
import edu.ucr.dblab.pflock.Utils.{round, save}

import scala.collection.mutable.ListBuffer
import scala.io.Source

object Points2Tikz {
  case class TikzPoint(x: Double, y: Double){
    override val toString: String = s"$x\t$y\n"
    val tex: String = s"\\node[point] at ($x, $y) {};\n"
  }

  def run(color: String = "blue", scale: String = "0.25")(implicit P: TikzParams, G: GeometryFactory): Unit = {

    val name = P.filename().split("\\.")(0)
    val main_file = new ListBuffer[String]()
    main_file.append("\\documentclass[border=1]{standalone}\n")
    main_file.append("\\usepackage{tikz}\n")
    main_file.append("\n")
    main_file.append("\\tikzset{\n")
    main_file.append(s"    point/.style={ draw, scale=$scale, color=$color, circle, fill }\n")
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
    val points = buffer.getLines().map{ line =>
      val arr = line.split("\t")
      val center = reader.read(arr(0)).asInstanceOf[Point]
      val x = center.getX
      val y = center.getY
      TikzPoint(x, y)
    }.toList
    buffer.close()

    save(s"${P.path()}/${name}_prime.tex"){
      points.map{ p =>
        val tex = p.copy(x = round(p.x/10.0, 2), y = round(p.y/10.0, 2)).tex
        println(tex)
        tex
      }
    }
  }
  def main(args: Array[String]): Unit = {
    implicit val P: TikzParams = new TikzParams(args)
    implicit val G: GeometryFactory = new GeometryFactory(new PrecisionModel(1e3))

    Points2Tikz.run()
  }
}