import lcm._

import scala.collection.mutable
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import collection.JavaConverters._
import SPMF.{AlgoFPMax, Transaction}

object FPMaxTester {
  def main(args: Array[String]): Unit = {
    import scala.io.Source

    val filename = args(0)
    val minsup = args(1).toInt

    val transactions = new ArrayBuffer[java.util.List[Integer]]()
    for (line <- Source.fromFile(filename).getLines) {
      transactions += line.split(" ").map(i => new Integer(i)).toList.asJava
    }
    val T = transactions.toList.asJava

    // Applying the algorithm
    val fpmax = new AlgoFPMax()
    val maximals = fpmax.runAlgorithm(T, minsup)
    //val maximals = itemsets.getItemsets(1)
    maximals.getLevels.asScala.flatMap{ level =>
      level.asScala.map{ itemset =>
        val items = itemset.getItems
        val support = itemset.getAbsoluteSupport
        
        s"[${items.mkString(" ")}: $support]"
      }
    }.foreach{println}
    fpmax.printStats()
  }
}
