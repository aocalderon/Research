import scala.collection.mutable
import scala.collection.mutable.ListBuffer

def reorder(list: List[Int], delta: Int): List[Int] = {
  list.grouped(delta).map(binarySearchOrder).reduce(_ union _)
}

def binarySearchOrder(list: List[Int]): List[Int] = {
  if(list.lengthCompare(3) < 0) return list.reverse
  val result = new ListBuffer[Int]()
  val queue = new mutable.Queue[(Int, Int)]()
  var lo = 0
  var hi = list.length - 1

  result += hi
  result += lo
  queue.enqueue((lo, hi))

  while(queue.nonEmpty){
    val pair = queue.dequeue()
    val lo = pair._1
    val hi = pair._2
    if(lo + 1 == hi){
    } else {
      val mi = Math.ceil(lo + (hi - lo) / 2.0).toInt
      result += mi
      queue.enqueue((mi, hi))
      queue.enqueue((lo, mi))
    }
  }

  result.toList.map(i => list(i))
}


val timestamps: List[Int] = List(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
val d: Int = 8

reorder(timestamps, d)





