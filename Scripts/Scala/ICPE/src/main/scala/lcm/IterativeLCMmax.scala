package lcm

import scala.collection.Map
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable._

object IterativeLCMmax {
  case class Pattern(items: List[Int], count: Int){
    override def toString: String = s"${items.mkString(" ")}: $count"
  }

  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  var n: Int = 0
  var N: Int = 0
  var debug: Boolean = true

  def run(T: List[Transaction], minsup: Int): List[Pattern] = {
    val buckets = occurrenceDeliver(T)

    var Ps = Stack[(Itemset, Map[Int, List[Transaction]])]()
    val P = new Itemset(List.empty)
    Ps.push((P, buckets))

    var patterns = ListBuffer[Pattern]()
    while(Ps.nonEmpty){
      n = n + 1
      val call = Ps.pop
      val P = call._1
      val buckets = call._2
      val I = buckets.keys.toList.sorted
      I.foreach{ e =>
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
            //if(P_prime.count >= minsup){
              patterns += Pattern(P_prime.items, P_prime.count)
            //}
            val T_prime = buckets(e).map(t => new Transaction(t.items))
            val I_prime = T_prime.flatMap(_.items.filter(_ > e)).distinct
            val buckets_prime = occurrenceDeliver(T_prime, I_prime, e)

            val call = (P_prime, buckets_prime)
            Ps.push(call)
          }
        }
      }
    }
    patterns.toList
  }

  private def isPPCExtension(P: Itemset, P_prime: Itemset, e: Integer): Boolean = {
    if(P_prime != P_prime.closure) return false
    if(e > P.contains(e) && P_prime.prefix(e - 1) == P.prefix(e - 1))
      true
    else
      false
  }

  def occurrenceDeliver(t_prime: List[Transaction], i_prime: List[Int], e: Int): scala.collection.Map[Int, List[Transaction]] = {
    var b_prime = new HashMap[Int, List[Transaction]]()
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
    var buckets = new HashMap[Int, List[Transaction]]()
    val uniqueElements = transactions.flatMap(_.items).distinct.sorted
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

  def savePatterns(patterns: List[String], output: String): Unit = {
    new java.io.PrintWriter(output) {
      write(patterns.mkString("\n") ++ "\n")
      close()
    }
  }

  def printN(): Unit = {
    println(s"Number of iterations: $n")
  }
}
