package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, PrecisionModel}
import org.locationtech.jts.io.WKTReader
import edu.ucr.dblab.pflock.Utils.{round, save}

import scala.io.Source

object Circles2Tikz {
  case class TikzCircle(x: Double, y: Double, r: Double, units: String = "", color: String = "black", line: String = "solid"){
    override val toString: String = s"$x\t$y\t$r\n"
    def options: String = List(color, line).mkString(",")
    val tex: String = s"\\draw[${options}] ($x, $y) circle [radius=${r}${units}]; \n"
  }
  def run(path: String, filename: String, output: String, minX: Double = 0.0, minY: Double = 0.0, color: String = "blue")(implicit G: GeometryFactory): Unit = {
    val reader = new WKTReader(G)
    val buffer = Source.fromFile(s"$path/$filename")
    val centers = buffer.getLines().map{ line =>
      val arr = line.split("\t")
      val center = reader.read(arr(0)).asInstanceOf[Point]
      val x = center.getX
      val y = center.getY
      val r = arr(1).toDouble
      TikzCircle(x, y, r, color = color, line = "dashed")
    }.toList
    buffer.close()

    save(s"$path/$output"){
      centers.map{ c =>
        val tex = c.copy(x = round(c.x - minX, 3), y = round(c.y - minY, 3)).tex
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

import org.rogach.scallop._

class TikzParams(args: Seq[String]) extends ScallopConf(args) {
  val path: ScallopOption[String] = opt[String] (default = Some("/tmp"))
  val filename: ScallopOption[String] = opt[String] (default = Some("cc.wkt"))
  val output: ScallopOption[String] = opt[String] (default = Some("circles.tex"))
  verify()
}

