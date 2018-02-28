import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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

	def main(args: Array[String]): Unit = {
		val list: List[Int] = List(0,1,2,3,4,5,6,7,8,9,10)
    val delta: Int = 5
    var start: Int = 0

    while(start + delta <= list.length) {
      val sublist = list.slice(start, start + delta)
      logger.info(reorder(sublist).toString())
      start = start + 1
    }
	}
}
