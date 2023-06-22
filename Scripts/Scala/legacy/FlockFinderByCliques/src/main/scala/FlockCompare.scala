package edu.ucr.dblab

import scala.io.Source
import java.io.PrintWriter

object FlockCompare {
  case class Differ(id: Int, flock: Set[Int], subsetOf: Int = -1){
    override def toString = {
      val flockStr = flock.toList.sorted.mkString(" ") 
      s"${id}] $flockStr subsetOf: $subsetOf"
    }
  }

  def main(args: Array[String]): Unit = {
    val file1 = args(0)
    val file2 = args(1)

    val flocks1 = getFlocks(file1)
    val flocks2 = getFlocks(file2)

    val differ1 = (flocks1 -- flocks2).zipWithIndex.map{ case(d, id) =>
      Differ(id, d.split(" ").map(_.toInt).toSet)
    }
    val differ2 = (flocks2 -- flocks1).zipWithIndex.map{ case(d, id) =>
      Differ(id, d.split(" ").map(_.toInt).toSet)
    }

    val A = for{
      a <- differ1
      b <- differ2 if a.flock.subsetOf(b.flock)
    } yield {
      a.copy(subsetOf = b.id)
    }

    val B = for{
      a <- differ2
      b <- differ1 if a.flock.subsetOf(b.flock)
    } yield {
      a.copy(subsetOf = b.id)
    }

    A.toList.sortBy(_.id).foreach(println)
    differ1.filterNot(d => A.map{_.flock}.contains(d.flock))
      .toList.sortBy(_.id).foreach{println}
    println
    B.toList.sortBy(_.id).foreach(println)
    differ2.filterNot(d => B.map{_.flock}.contains(d.flock))
      .toList.sortBy(_.id).foreach{println}

  }

  def getFlocks(filename: String): Set[String] = {
    val buff = Source.fromFile(filename)
    val flocks = buff.getLines.map{ line =>
      val arr = line.split(", ")
      arr(2)
    }.toSet
    buff.close
    flocks
  }
}
