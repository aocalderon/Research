package SPMF.ScalaLCM

import scala.collection.mutable
import scala.collection.Map
import scala.collection.mutable.ListBuffer
import scala.util.Try

object LCMmax {
  case class Item(item: Int, count: Int)

  var buckets: Map[Int, List[Transaction]] = Map.empty
  var uniqueElements: List[Int] = List.empty[Int]
  var patterns: ListBuffer[String] = new ListBuffer[String]()
  var debug: Boolean = true

  def main(args: Array[String]): Unit = {
    import scala.io.Source

    val filename = args(0)
    val output = args(1)
    debug = Try(args(2).toBoolean).getOrElse(false)

    val transaction_buffer: ListBuffer[Transaction] = new ListBuffer()
    for (line <- Source.fromFile(filename).getLines) {
      transaction_buffer += new Transaction(line.split(" ").map(_.toInt).toList)
    }
    val T = transaction_buffer.toList

    uniqueElements = T.flatMap(_.items).distinct.sorted
    buckets = occurrenceDeliver(T)
    
    val P = new Itemset(List.empty)
    backtracking(P, buckets)
    
    new java.io.PrintWriter(output) {
      write(patterns.toList.mkString("\n"))
      close()
    }
  }

  def backtracking(P: Itemset, buckets: Map[Int, List[Transaction]]): Itemset = {
    val I = buckets.keys.toList.sorted
    if(I.nonEmpty){
      for(e <- I){
        if(P.nonEmpty && P.contains(e) >= 0) {
        } else {
          var P_prime = P.U(e)
          P_prime.count = buckets(e).size
          val d = buckets(e).toSet.map{ t: Transaction =>
            new Transaction(t.items, t.contains(e))
          }
          P_prime.setDenotation(d)
          P_prime = P_prime.getClosure
          val isPPC = isPPCExtension(P, P_prime, e)

          if(isPPC){
            if(P_prime.count == 1){
              var pattern = s"${P_prime.toString}"
              patterns += pattern
              if(debug) println(pattern)
            }
            val T_prime = buckets(e).map(t => new Transaction(t.items))
            val I_prime = T_prime.flatMap(_.items.filter(_ > e)).distinct
            val buckets_prime = occurrenceDeliver(T_prime, I_prime, e)

            backtracking(P_prime, buckets_prime)
          }
        }
      }
    }

    P
  }

  private def isPPCExtension(P: Itemset, P_prime: Itemset, e: Integer): Boolean = {
    if(P_prime != P_prime.closure) return false
    P.clo_tail = P.contains(e)
    //if(e > P.tail && P_prime.prefix(e - 1) == P.prefix(e - 1))
    if(e > P.clo_tail && P_prime.prefix(e - 1) == P.prefix(e - 1))
      true
    else
      false
  }

  def occurrenceDeliver(t_prime: List[Transaction], i_prime: List[Int], e: Int): scala.collection.Map[Int, List[Transaction]] = {
    var b_prime = new mutable.HashMap[Int, List[Transaction]]()
    for (t <- t_prime) {
      for (i <- i_prime) {
        if(t.contains(i) >= 0){
          b_prime.get(i) match {
            case Some(ts: List[Transaction]) => b_prime.update(i, ts :+ t)
            case None => b_prime += (i -> List(t))
          }
        }
      }
    }
    b_prime.mapValues(_.distinct)
  }

  def occurrenceDeliver(transactions: List[Transaction]): scala.collection.Map[Int, List[Transaction]] = {
    var buckets = new mutable.HashMap[Int, List[Transaction]]()
    for (transaction <- transactions) {
      for (element <- uniqueElements) {
        if(transaction.contains(element) >= 0){
          buckets.get(element) match {
            case Some(ts: List[Transaction]) => buckets.update(element, ts :+ transaction)
            case None => buckets += (element -> List(transaction))
          }
        }
      }
    }
    buckets.mapValues(_.distinct)
  }

  def logging(msg: String, timer: Long, n: Long = 0, tag: String = ""): Unit ={
    println("%-50s | %6.2fs | %6d %s".format(msg, (System.currentTimeMillis() - timer)/1000.0, n, tag))
  }
}
