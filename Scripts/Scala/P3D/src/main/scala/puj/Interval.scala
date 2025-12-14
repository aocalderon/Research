package puj

import scala.util.Random
import java.util.Arrays

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.scala.Logging

class Interval(val index: Int, val begin: Int, val end: Int, val capacity: Int = 0) extends Serializable {
  val duration: Int = end - begin

  override def toString(): String = s"$index: [$begin $end] ($duration)"

  val toText: String = s"${toString()}\n"
}

object Interval extends Logging {

  def apply(index: Int, begin: Int, end: Int, capacity: Int = 0): Interval = 
    new Interval(index, begin, end, capacity)

  def findInterval(bounds: Array[Int], query: Integer)(implicit intervals: Map[Int, Interval]): Interval = {
    val index = Arrays.binarySearch(bounds, query)
    val position = if(index < 0) -(index) - 2 else index
    intervals(position)
  }

  def main(args: Array[String]): Unit = {
    val n = 300
    val nIntervals = 25
    val size = 25000

    val I = (0 +: (0 to nIntervals - 2).map{ i => Random.nextInt(n) }.toArray.sorted :+ n).distinct

    implicit val intervals = I.zip(I.tail).zipWithIndex.map{ case(t, i) =>
      i -> Interval(i, t._1, t._2 - 1)
    }.toMap
    val bounds = intervals.values.map(_.begin).toArray.sorted

    val Q = (0 to size).map{ i =>
      Random.nextInt(n)
    }.toList

    Timer.time("Foreach"){
      Q foreach { query =>
        val interval = findInterval(bounds, query)
      }
    }

    Timer.time("While"){
      var i = 0
      while(i < Q.size){
        val query = Q(i)
        val interval = findInterval(bounds, query)
        i += 1
      }
    }
  }
}
