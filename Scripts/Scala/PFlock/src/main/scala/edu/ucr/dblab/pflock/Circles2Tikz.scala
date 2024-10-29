package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.Utils.{round, save}
import org.locationtech.jts.geom.{GeometryFactory, Point, PrecisionModel}
import org.locationtech.jts.io.WKTReader

import scala.collection.mutable.ListBuffer
import scala.io.Source

object Circles2Tikz {
  case class TikzCircle(x: Double, y: Double, r: Double){
    override val toString: String = s"$x\t$y\t$r\n"

    val tex: String = s"\\draw[circlep] ($x, $y) circle [radius=$r];\n"
  }
  def run(color: String = "blue", style: String = "solid")
         (implicit P: TikzParams, G: GeometryFactory): Unit = {

    val name = P.filename().split("\\.")(0)
    val main_file = new ListBuffer[String]()
    main_file.append("\\documentclass[border=1]{standalone}\n")
    main_file.append("\\usepackage{tikz}\n")
    main_file.append("\n")
    main_file.append("\\tikzset{\n")
    main_file.append(s"\t circlep/.style={ draw, $style, color=$color }\n")
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
    val centers = buffer.getLines().map{ line =>
      val arr = line.split("\t")
      val center = reader.read(arr(0)).asInstanceOf[Point]
      val x = center.getX
      val y = center.getY
      val r = arr(1).toDouble
      TikzCircle(x, y, r)
    }.toList
    buffer.close()

    save(s"${P.path()}/${name}_prime.tex"){
      centers.map{ c =>
        val tex = c.copy(x = round(c.x, 3), y = round(c.y, 3)).tex
        println(tex)
        tex
      }
    }

  }
  def main(args: Array[String]): Unit = {
    implicit val P: TikzParams = new TikzParams(args)
    implicit val G: GeometryFactory = new GeometryFactory(new PrecisionModel(1e3))

    Circles2Tikz.run()
  }
}

import org.rogach.scallop._

class TikzParams(args: Seq[String]) extends ScallopConf(args) {
  val path: ScallopOption[String] = opt[String] (default = Some("/tmp"))
  val filename: ScallopOption[String] = opt[String] (default = Some("cc.wkt"))
  val output: ScallopOption[String] = opt[String] (default = Some("circles.tex"))
  verify()
}

