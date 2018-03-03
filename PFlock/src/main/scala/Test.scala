import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import collection.immutable.HashSet
import SPMF.AlgoFPMax
import scala.collection.JavaConverters._



object Test {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  def reorder(list: List[Int]): List[Int] = {
    val queue = new mutable.Queue[(Int, Int)]()
    val result = new ListBuffer[Int]()
    var lo = 0
    var hi = list.length - 1

    result += lo
    result += hi
    queue.enqueue((lo, hi))

    while(!queue.isEmpty){
      val pair = queue.dequeue()
      val lo = pair._1
      val hi = pair._2
      if(lo + 1 == hi){
      } else {
        val mi = lo + (hi - lo) / 2
        result += mi
        queue.enqueue((lo, mi))
        queue.enqueue((mi, hi))
      }
    }

    result.toList.map(i => list(i))
  }

  def reduceDisks(a: Set[String], b: Set[String]): Set[String] ={
    val transactions = a.union(b).map { disk => disk.split(" ").map(new Integer(_)).toList.asJava}.toList.asJava
    val algorithm = new AlgoFPMax
    val maximals = algorithm.runAlgorithm(transactions, 1)

    maximals.getItemsets(1).asScala.map(m => m.asScala.toList.sorted.mkString(" ")).toSet
  }

	def main(args: Array[String]): Unit = {
    val d0: Set[String] = HashSet("0 9")
		val d1: Set[String] = HashSet("1 2 3 4 5")
    val d2: Set[String] = HashSet("1 3 4 5")
    val d3: Set[String] = HashSet("2 3 4")
    val d4: Set[String] = HashSet("3 4 5")
    val d5: Set[String] = HashSet("6 7 8 9")
    val d6: Set[String] = HashSet("1 2 3 4 5")
    val d7: Set[String] = HashSet("7 8")
    val d8: Set[String] = HashSet("8 9")
    val d9: Set[String] = HashSet("6 7")

    var disks: List[Set[String]] = List(d0,d1,d2,d3,d4,d5,d6,d7,d8,d9)
    val result = disks.reduce{ (a, b) => reduceDisks(a, b) }

    result.foreach(println)
	}
}
