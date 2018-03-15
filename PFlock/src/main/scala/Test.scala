import SPMF.AlgoFPMax
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Test {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  def reorder(list: List[Int]): List[Int] = {
    if(list.lengthCompare(1) == 0) return list
    val queue = new mutable.Queue[(Int, Int)]()
    val result = new ListBuffer[Int]()
    var lo = 0
    var hi = list.length - 1

    result += lo
    result += hi
    queue.enqueue((lo, hi))

    while(queue.nonEmpty){
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
    val timestamps: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val delta: Int = 4

    val windows = timestamps.grouped(delta).toList.map(reorder)
    for(window <- windows){
      for(timestamp <- window){
        logger.info(s"$timestamp")
      }
    }
	}
}
