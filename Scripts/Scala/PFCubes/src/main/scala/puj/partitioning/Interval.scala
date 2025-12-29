package puj.partitioning

import scala.util.Random
import java.util.Arrays

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.scala.Logging
import scala.annotation.tailrec

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

/** An interval defined by its begin and end instants.
  *
  * @param index
  * @param begin
  * @param end
  * @param capacity
  */
case class Interval(index: Int, begin: Int, end: Int, capacity: Int = 0) {
  val duration: Int = end - begin

  override def toString(): String = s"$index -> [$begin $end]:$duration [$capacity]"

  val toText: String = s"${toString()}\n"
}

object Interval extends Logging {

  def apply(index: Int, begin: Int, end: Int, capacity: Int = 0): Interval =
    new Interval(index, begin, end, capacity)

  /** Finds the interval that contains a given time instant.
    * @param time_instant
    * @param intervals
    * @return
    *   the time interval containing the time instant
    */
  def findTimeInstant(time_instant: Int)(implicit intervals: Map[Int, Interval]): Interval = {
    val temporal_bounds = intervals.values.map(_.begin).toArray.sorted
    val index           = Arrays.binarySearch(temporal_bounds, time_instant)
    val position        = if (index < 0) -(index) - 2 else index
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

  /** Partitions a sequence of time instances into intervals of a specified maximum size.
    *
    * @param numbers
    *   The input sequence of time instances to be partitioned.
    * @param n
    *   The maximum number of time instances allowed per interval.
    * @return
    *   A list of Intervals.
    */
  def intervalsBySize(numbers: Seq[Int], n: Int): Map[Int, Interval] = {
    val grouped = numbers.grouped(n).toList
    grouped.zipWithIndex.map { case (group, idx) =>
      val begin = group.head
      val end   = group.last + 1
      idx -> Interval(idx, begin, end)
    }.toMap
  }

  /** Cuts a sequence into n approximately equal parts and maps each element to its part index.
    * @param xs
    *   The input sequence to be cut.
    * @param n
    *   The number of parts to cut the sequence into.
    * @param k
    *   An optional offset to start indexing parts from.
    * @tparam A
    *   The type of elements in the input sequence.
    * @return
    *   A map where each element from the input sequence is associated with its part index.
    */
  def cut[A](xs: Seq[A], n: Int, k: Int = 0): Map[A, Int] = {
    val m       = xs.length
    val targets = (0 to n).map { x => math.round((x.toDouble * m) / n).toInt }

    @tailrec
    def snip(xs: Seq[A], ns: Seq[Int], got: Vector[Seq[A]]): Vector[Seq[A]] = {
      if (ns.length < 2) got
      else {
        val (i, j) = (ns.head, ns.tail.head)
        snip(xs.drop(j - i), ns.tail, got :+ xs.take(j - i))
      }
    }
    val intervals = snip(xs, targets, Vector.empty)
    intervals.zipWithIndex.flatMap { case (interval, id) =>
      interval.map { x => (x, id) }
    }.toMap
  }
}
