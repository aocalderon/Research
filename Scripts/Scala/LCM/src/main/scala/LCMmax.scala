import scala.collection.mutable
import scala.collection.Map
import scala.collection.mutable.ListBuffer

object LCMmax {
  case class Item(item: Int, count: Int)

  var buckets: Map[Int, List[Transaction]] = Map.empty
  var uniqueElements: List[Int] = List.empty[Int]
  var patterns: ListBuffer[String] = new ListBuffer[String]()

  def main(args: Array[String]): Unit = {
    import scala.io.Source

    val filename = args(0)
    val transaction_buffer: ListBuffer[Transaction] = new ListBuffer()
    for (line <- Source.fromFile(filename).getLines) {
      transaction_buffer += new Transaction(line.split(" ").map(_.toInt).toList)
    }
    val T = transaction_buffer.toList

    uniqueElements = T.flatMap(_.items).distinct.sorted
    buckets = occurrenceDeliver(T)
    
    val P = new Itemset(List.empty)
    backtracking(P, buckets)
    
    val output = args(1)
    new java.io.PrintWriter(output) {
      write(patterns.toList.mkString("\n"))
      close()
    }
    patterns.toList.foreach(println)
  }

  def backtracking(P: Itemset, buckets: Map[Int, List[Transaction]]): Itemset = {
    //val timer = System.currentTimeMillis()
    val I = buckets.keys.toList
    if(I.nonEmpty){
      for(e <- I.sorted){
        //println(s"Watching $e...")
        if(P.nonEmpty && P.contains(e) >= 0) {
        } else {
          val P_prime = P.U(e)
          P_prime.count = buckets(e).size
          val d = buckets(e).toSet.map{ t: Transaction =>
            new Transaction(t.items, t.contains(e))
          }
          P_prime.setDenotation(d)

          //val isPPC = P.prefix(e).equals(P_prime.prefix(e)) && P_prime.items.equals(P_prime.closure)
          //println(s"Is $P_prime PPC? $isPPC")

          val isPPC = isPPCExtension(P_prime, e)

          if(isPPC /* && P_prime.count == 1 */ ){
            println(new Itemset(P_prime.closure).toString)
            patterns += P_prime.toString
          }

          val T_prime = buckets(e).map(t => new Transaction(t.items/*.filter(_ > e)*/))
          val I_prime = T_prime.flatMap(_.items.filter(_ > e)).distinct
          val buckets_prime = occurrenceDeliver(T_prime, I_prime, e)

          //buckets_prime.keys.map( k => s"$k : ${buckets_prime.get(k).mkString(" ")}" ).foreach(println)
          //println()
          val newP = new Itemset(P_prime.closure)
          //println(newP.toString)

          backtracking(newP, buckets_prime)
        }
      }
    }
    //logging(s"Backtracking for $P...", timer, tag=s"$P")

    P
  }

  def isItemInAllTransactionsExceptFirst(denotation: Set[Transaction], item: Integer): Boolean = {
    var i = 1
    while ( {
      i < denotation.size
    }) {
      if (denotation.toList(i).contains(item)  == -1) return false

      {
        i += 1; i - 1
      }
    }
    true
  }

  private def isPPCExtension(p: Itemset, e: Integer): Boolean = { // We do a loop on each item i of the first transaction
    if (p.denotation.isEmpty) return false
    val firstTrans: Transaction = p.denotation.toList.head
    val firstTransaction = firstTrans.items
    var i: Int = 0
    while (i < firstTrans.offset) {
      val item: Integer = firstTransaction(i)
      // if p does not contain item i < e and item i is present in all transactions,
      // then it PUe is not a ppc
      if (item < e && (p.nonEmpty() || p.contains(e) == -1) && isItemInAllTransactionsExceptFirst(p.denotation, item)) { //&& isItemInAllTransactions(transactionsPe, item)) {
        return false
      }
      i += 1
    }
    true
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
