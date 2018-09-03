import scala.collection.mutable
import scala.collection.Map

object LCMmax {
  case class Item(item: Int, count: Int)

  var buckets: Map[Int, List[Transaction]] = Map.empty
  var uniqueElements: List[Int] = List.empty[Int]

  def main(args: Array[String]): Unit = {
    val T: List[Transaction] = List(
      new Transaction(List(1, 2, 5, 7, 9)),
      new Transaction(List(1, 3, 5, 7, 9)),
      new Transaction(List(2, 4, 1, 3)),
      new Transaction(List(1, 3, 4, 5, 6)),
      new Transaction(List(1, 2)),
      new Transaction(List(2, 1)),
      new Transaction(List(1, 7, 2, 3, 4, 5, 8, 9)),
      new Transaction(List(6, 1, 2)),
      new Transaction(List(4, 5, 6)),
      new Transaction(List(8, 2, 5)),
      new Transaction(List(9, 2, 1)),
      new Transaction(List(1, 2, 4, 8, 9))
    )

    uniqueElements = T.flatMap(_.items).distinct.sorted
    buckets = occurrenceDeliver(T)
    
    val P = new Itemset(List.empty)
    backtracking(P, buckets)
  }

  def backtracking(P: Itemset, buckets: Map[Int, List[Transaction]]): Unit = {
    //val I = buckets.map(b => Item(b._1, b._2.size)).toList.sortBy(i => i.count * -1).map(_.item)
    val I = buckets.map(_._1).toList.sorted

    println(P)

    I.filter(_ > P.tail()).foreach{ e =>
      val P_prime = P.U(e)
      val T_prime = buckets(e).map(t => new Transaction(t.items.filter(_ != e)))
      val I_prime = T_prime.flatMap(_.items).distinct.sorted
      val buckets_prime = occurrenceDeliver(T_prime, I_prime)

      backtracking(P_prime, buckets_prime)
    }
  }

  def occurrenceDeliver(t_prime: List[Transaction], i_prime: List[Int]): scala.collection.Map[Int, List[Transaction]] = {
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
}
