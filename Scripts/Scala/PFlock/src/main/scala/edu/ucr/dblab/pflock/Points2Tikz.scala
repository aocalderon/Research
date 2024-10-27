package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{GeometryFactory, Point, PrecisionModel}
import org.locationtech.jts.io.WKTReader
import edu.ucr.dblab.pflock.Utils.{round, save}

import scala.io.Source

object Points2Tikz {
  case class TikzPoint(x: Double, y: Double, scale: Double = 0.25, units: String = "mm", color: String = "black"){
    override val toString: String = s"$x\t$y\n"
    def options: String = List("fill", s"color=${color}", s"scale=${scale}").mkString(",")
    val tex: String = s"\\node[draw,circle,${options}] at ($x, $y) {};\n"
  }

  def run(epsilon: Double, color: String = "blue")(implicit P: TikzParams, G: GeometryFactory): (Double, Double) = {

    val reader = new WKTReader(G)
    val buffer = Source.fromFile(s"${P.path()}/${P.filename()}")
    val points = buffer.getLines().map{ line =>
      val arr = line.split("\t")
      val center = reader.read(arr(0)).asInstanceOf[Point]
      val x = center.getX
      val y = center.getY
      TikzPoint(x, y, color = color)
    }.toList
    buffer.close()

    val minX = points.minBy(_.x).x
    val minY = points.minBy(_.y).y
    val maxX = points.maxBy(_.x).x
    val maxY = points.maxBy(_.y).y

    save(s"${P.path()}/${P.output()}"){
      s"\\draw [draw=black, color=gray!15, dashed] (${-epsilon}, ${-epsilon}) rectangle (${maxX-minX+epsilon}, ${maxY-minY+epsilon});" +:
      points.map{ p =>
        val tex = p.copy(x = round(p.x - minX, 3), y = round(p.y - minY, 3)).tex
        println(tex)
        tex
      }
    }

    (minX, minY)
  }
  def main(args: Array[String]): Unit = {
    implicit val P: TikzParams = new TikzParams(args)
    implicit val G: GeometryFactory = new GeometryFactory(new PrecisionModel(1e3))

    val (minX, minY) = Points2Tikz.run(10.0)
    Lines2Tikz.run(P.path(), "edgesPL.wkt", "pl.tex", minX = minX, minY = minY)
    Circles2Tikz.run(P.path(), "edgesCC.wkt", "cc.tex", minX = minX, minY = minY, color = "cyan!25")
    Circles2Tikz.run(P.path(), "edgesMC.wkt", "mc.tex", minX = minX, minY = minY, color = "red")
    Polygons2Tikz.run(P.path(), "edgesCH.wkt", "ch.tex", minX = minX, minY = minY)
  }
}