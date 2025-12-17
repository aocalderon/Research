package puj

import scala.util.Random
import java.util.Arrays

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.scala.Logging

/** A bin of a histogram that count how many entries per instant in a sequence.
  *
  * @param instant
  *   A particular instant.
  * @param count
  *   The count of entries in this instant.
  */
case class Bin(instant: Int, count: Int) {
  override def toString(): String = s"[$instant, $count]"
}

class Interval(val index: Int, val begin: Int, val end: Int, val capacity: Int = 0) extends Serializable {
  val duration: Int = end - begin

  override def toString(): String = s"$index -> [$begin $end]:$duration [$capacity]"

  val toText: String = s"${toString()}\n"
}

object Interval extends Logging {

  def apply(index: Int, begin: Int, end: Int, capacity: Int = 0): Interval =
    new Interval(index, begin, end, capacity)

  def findInterval(bounds: Array[Int], query: Integer)(implicit intervals: Map[Int, Interval]): Interval = {
    val index    = Arrays.binarySearch(bounds, query)
    val position = if (index < 0) -(index) - 2 else index
    intervals(position)
  }

  /** Groups a sequence of bins (instants and their counts) into sub-sequences from left to right, such that the sum of each group does not exceed a maximum limit. The process is greedy: it maximizes the size of the current group before starting a new one. The relative order of elements is preserved.
    *
    * @param numbers
    *   The input sequence of integers.
    * @param capacity
    *   The maximum allowed sum for any group.
    * @return
    *   A list of lists, where each inner list is a valid group (except for elements that individually exceed the maxSum, which are handled with a warning).
    */
  def groupInstants(numbers: Seq[Bin], capacity: Double): List[List[Bin]] = {
    // We use a foldLeft to process the list sequentially and maintain state.
    // The state (accumulator) is a tuple: (completedGroups, currentGroup, currentSum)
    val initialState = (List.empty[List[Bin]], List.empty[Bin], 0)

    val (completedGroups, finalGroup, _) = numbers.foldLeft(initialState) { case ((groups, currentGroup, currentSum), bin) =>

      // 1. Sanity Check: If an individual number exceeds the maximum sum,
      // it must form its own group. We complete the current group and start
      // a new one containing only the violating number.
      val number = bin.count
      if (number > capacity) {
        // println(s"Warning: Element $number exceeds maxSum $maxSum. It will be placed in a separate group.")
        val updatedGroups = if (currentGroup.nonEmpty) groups :+ currentGroup else groups
        // Start a new group with the single, violating number and immediately complete it.
        (updatedGroups :+ List(bin), List.empty[Bin], 0)
      }

      // 2. Standard case: Check if adding the number is within the limit
      else if (currentSum + number <= capacity) {
        // If the sum is within the limit, add the number to the current group
        // Note: using :+ on List for simple append is fine for small lists,
        // but for highly performance-critical code on very large lists,
        // one might prefer prepending and reversing at the end.
        (groups, currentGroup :+ bin, currentSum + number)
      } else {
        // 3. Limit exceeded: The current group is complete.
        // Start a new group with the current number.
        (groups :+ currentGroup, List(bin), number)
      }
    }

    // After folding, we must add the last group being built (finalGroup), if it's not empty.
    if (finalGroup.nonEmpty) completedGroups :+ finalGroup else completedGroups
  }

  def main(args: Array[String]): Unit = {
    val n          = 300
    val nIntervals = 25
    val size       = 25000

    val I = (0 +: (0 to nIntervals - 2).map { i => Random.nextInt(n) }.toArray.sorted :+ n).distinct

    implicit val intervals = I
      .zip(I.tail)
      .zipWithIndex
      .map { case (t, i) =>
        i -> Interval(i, t._1, t._2 - 1)
      }
      .toMap
    val bounds = intervals.values.map(_.begin).toArray.sorted

    val Q = (0 to size).map { i =>
      Random.nextInt(n)
    }.toList

    Timer.time("Foreach") {
      Q foreach { query =>
        val interval = findInterval(bounds, query)
      }
    }

    Timer.time("While") {
      var i = 0
      while (i < Q.size) {
        val query    = Q(i)
        val interval = findInterval(bounds, query)
        i += 1
      }
    }
  }
}
