package edu.ucr.dblab;

import java.io.PrintWriter
import scala.io.Source
import sys.process._
import scala.language.postfixOps

object tsv2dot {

  case class Point(id: Int, x: Double, y: Double)

  def main(args: Array[String]): Unit = {
    val input  = args(0)
    val epsilon = args(1).toDouble
    val output = args(2)

    val pointsBuff = Source.fromFile(input)
    val points = pointsBuff.getLines.map{ line =>
      val arr = line.split("\t")
      val id = arr(0).toInt
      val x = arr(1).toDouble
      val y = arr(2).toDouble

      Point(id, x, y)
    }.toVector
    pointsBuff.close

    val pairs = for{
      a <- points
      b <- points if distance(a, b) <= epsilon && a.id < b.id
    } yield {
      (a, b)
    }

    val nodes = points.map{ p =>
      val id = p.id
      val x = p.x
      val y = p.y
      s"""$id [pos = "$x, $y!"]\n"""
    }
    val edges = pairs.map{ case(a, b) =>
      val aid = a.id
      val bid = b.id

      s"$aid -- $bid\n"
    }

    val dot = ("graph G {\n" +: nodes) ++ (edges :+ "}\n")

    val f = new PrintWriter(output)
    f.write(dot.mkString(""))
    f.close

    val name = output.split("\\.").head
    s"neato ${name}.dot -s1 -Tpdf -o ${name}.pdf" ! 
  }

  private def distance(a: Point, b: Point): Double = {
    math.sqrt(math.pow(a.x - b.x, 2) + math.pow(a.y - b.y, 2))
  }

}
