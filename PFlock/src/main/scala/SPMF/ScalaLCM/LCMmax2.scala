package SPMF.ScalaLCM

import scala.collection.mutable
import scala.collection.Map
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._
import org.apache.spark.sql.simba.SimbaSession
import collection.JavaConverters._
import org.slf4j.{Logger, LoggerFactory}

object LCMmax2 {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  case class Disk(pids: String)
  case class Items(ids: List[Int], closure: List[Int] = List.empty)
  case class OccurrenceDelivery(p: Int, transaction: List[Int])

  var buckets: Map[Int, List[Transaction]] = Map.empty
  var uniqueElements: List[Int] = List.empty[Int]
  var patterns: ListBuffer[String] = new ListBuffer[String]()
  var n: Int = 0
  var debug: Boolean = false

  def main(args: Array[String]): Unit = {
    val conf = new ConfLCMmax2(args)
    val input      = conf.input() 
    val output     = conf.output()
    val master     = conf.master()
    val partitions = conf.partitions()
    val cores      = conf.cores()
    val debug      = conf.debug()
    val print      = conf.print()

    val simba = SimbaSession.builder()
      .master(master)
      .appName("LCMmax2")
      .config("simba.index.partitions", partitions)
      .config("spark.cores.max", cores)
      .getOrCreate()
    import simba.implicits._

    // Reading file...
    var timer = System.currentTimeMillis()
    val data = simba.read.option("header", "false")
      .csv(input)
      .map(d => Disk(d.getString(1)))
      .repartition(partitions)
      .cache()
    val nData = data.count()
    logging("Reading file...", timer, nData, "records")

    val T = data.map(d => Items(d.pids.split(" ").map(_.toInt).toList.sorted))
    T.show(10, truncate=false)

    val I = T.flatMap(_.ids).distinct().toDF("id")
    val n = I.count().toInt
    I.show(10,false)
    println(s"Number of partitions: ${I.rdd.getNumPartitions}")

    val T_explode = T.withColumn("id", explode($"ids"))
    val groupExpr = 'id as "group"
    val OD = I.join(T_explode, "id").repartition(n, groupExpr)
    OD.show(50, false)
    println(s"Number of partitions: ${OD.rdd.getNumPartitions}")

    val OD2 = I.join(T_explode, "id")
      .map(j => (j.getInt(0), j.getList[Int](1).asScala.toList)).rdd
      .partitionBy(new IDPartitioner(n))
      .mapPartitions { rows =>
        val r = rows.toList
        val closure = r.map(_._2).reduce( (a,b) => a.intersect(b))
        val conditional = r.map{ t =>
          val P = closure
          val e = t._1
          val P_prime = t._2.diff(closure)
          (P, e, P_prime)
        }.filter(_._3.size > 0)
        conditional.toIterator
      }
    OD2.foreach(println)
    println(s"Number of partitions: ${OD2.getNumPartitions}")

    

    /*
    if(output.isEmpty()){
      results.foreach(println)
      println()
    } else {
      new java.io.PrintWriter(output) {
        write(results.mkString("\n") ++ "\n")
        close()
      }
    }
    */
  }

  def run(T: List[Transaction]): List[String] = {
    uniqueElements = T.flatMap(_.items).distinct.sorted
    buckets = occurrenceDeliver(T)
    
    val P = new Itemset(List.empty)
    backtracking(P, buckets)

    patterns.toList
  }

  def backtracking(P: Itemset, buckets: Map[Int, List[Transaction]]): Unit = {
    n = n + 1
    val I = buckets.keys.toList.sorted
    if(I.nonEmpty){
      for(e <- I){
        if(P.nonEmpty && P.contains(e) >= 0) {
        } else { //33564,11452
          val time1 = System.currentTimeMillis()
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
          if(debug) println(s"$n,${System.currentTimeMillis() - time1}")
        }
      }
    }
  }

  private def isPPCExtension(P: Itemset, P_prime: Itemset, e: Integer): Boolean = {
    if(P_prime != P_prime.closure) return false
    P.clo_tail = P.contains(e)
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

  def logging(msg: String, timer: Long, n: Long = 0, tag: String = ""): Unit = {
    if(n == 0){
      logger.info("%-50s | %6.2f".format(msg, (System.currentTimeMillis() - timer)/1000.0))
    } else {
      logger.info("%-50s | %6.2f | %6d %s".format(msg, (System.currentTimeMillis() - timer)/1000.0, n, tag))
    }
  }

  def printN(): Unit = {
    println(s"Number of recursions: $n")
  }
}

import org.apache.spark.Partitioner

class IDPartitioner(override val numPartitions: Int)  extends Partitioner {
  override def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Int]
    return k % numPartitions
  }

  override def equals(other: Any): Boolean = {
    other match {
      case obj: IDPartitioner => obj.numPartitions == numPartitions
      case _ => false
    }
  }
}

import org.rogach.scallop.{ScallopConf, ScallopOption}

class ConfLCMmax2(arguments: Seq[String]) extends ScallopConf(arguments) {
  val input:      ScallopOption[String]  = opt[String]  (required = true)
  val output:     ScallopOption[String]  = opt[String]  (default = Some(""))
  val master:     ScallopOption[String]  = opt[String]  (default = Some("local[*]"))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(16))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(7))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val print:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
