package SPMF.ScalaLCM

import scala.collection.Map
import org.apache.spark.sql.simba.SimbaSession
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable._

object IterativeLCMmax {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  var n: Int = 0

  def main(args: Array[String]): Unit = {
    val conf = new ConfIterativeLCMmax(args)
    val input      = conf.input() 
    val output     = conf.output()
    val master     = conf.master()
    val partitions = conf.partitions()
    val cores      = conf.cores()
    val debug      = conf.debug()
    val print      = conf.print()
    val N          = conf.iterations()

    val simba = SimbaSession.builder()
      .master(master)
      .appName("IterativeLCMmax")
      .config("simba.index.partitions", partitions)
      .config("spark.cores.max", cores)
      .getOrCreate()
    import simba.implicits._

    // Reading file...
    var timer = System.currentTimeMillis()
    val data = simba.read.option("header", "false")
      .csv(input)
      .map(_.getString(0))
      .repartition(partitions)
      .cache()
    val nData = data.count()
    if(debug) logging("Reading file...", timer, nData, "records")
    if(debug) data.show(10, truncate=false)

    val T = data.collect().map{ line =>
      new Transaction(line.split(" ").map(_.toInt).toList)
    }.toList
    val buckets = occurrenceDeliver(T)

    var Ps = Stack[(Itemset, Map[Int, List[Transaction]])]()
    /*val I = buckets.keys.toList.sorted.map{ e =>
      val P = new Itemset(List(e))
      val call = (P, buckets)
      Ps.push(call)
     }*/
    val P = new Itemset(List.empty)
    Ps.push((P, buckets))

    timer = System.currentTimeMillis()
    var patterns = ListBuffer[String]()
    while(Ps.nonEmpty){
      n = n + 1
      val call = Ps.pop
      val P = call._1
      val buckets = call._2
      val I = buckets.keys.toList.sorted
      I.foreach{ e=>
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
              //println(pattern)
            }
            val T_prime = buckets(e).map(t => new Transaction(t.items))
            val I_prime = T_prime.flatMap(_.items.filter(_ > e)).distinct
            val buckets_prime = occurrenceDeliver(T_prime, I_prime, e)

            val call = (P_prime, buckets_prime)
            if(debug) System.out.print(printProgressBar(n, N))
            Ps.push(call)
          }
        }
      }
    }
    if(debug) println("")
    if(print) patterns.toList.distinct.foreach(println)
    if(output.nonEmpty) savePatterns(patterns.toList.distinct, output)
    if(debug) logging("Finding patterns...", timer, patterns.length, "patterns")
    if(debug) printN()
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

  def logging(msg: String, timer: Long, n: Long = 0, tag: String = ""): Unit = {
    if(n == 0){
      logger.info("%-50s | %6.2f".format(msg, (System.currentTimeMillis() - timer)/1000.0))
    } else {
      logger.info("%-50s | %6.2f | %6d %s".format(msg, (System.currentTimeMillis() - timer)/1000.0, n, tag))
    }
  }

  def savePatterns(patterns: List[String], output: String): Unit = {
    new java.io.PrintWriter(output) {
      write(patterns.mkString("\n") ++ "\n")
      close()
    }
  }

  def printProgressBar(n:Int, N:Int, c:String = "#", l:Int = 100): String = {
    val factor = ((n * l) / N).toInt
    val blanks = l - factor - 1
    val b = " "
    s"[${c * factor}${b * blanks}]\r"
  }

  def printN(): Unit = {
    println(s"Number of iterations: $n")
  }
}

import org.rogach.scallop.{ScallopConf, ScallopOption}

class ConfIterativeLCMmax(arguments: Seq[String]) extends ScallopConf(arguments) {
  val input:      ScallopOption[String]  = opt[String]  (required = true)
  val output:     ScallopOption[String]  = opt[String]  (default = Some(""))
  val master:     ScallopOption[String]  = opt[String]  (default = Some("local[*]"))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(16))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(7))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val print:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val iterations: ScallopOption[Int]     = opt[Int]     (default = Some(100))


  verify()
}
