import lcm._

import scala.collection.mutable
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object IterativeScalaLCMTester {
  def main(args: Array[String]): Unit = {
    import scala.io.Source

    val filename = args(0)
    val minsup = args(1).toInt

    val transaction_buffer = new ArrayBuffer[Transaction]()
    for (line <- Source.fromFile(filename).getLines) {
      transaction_buffer += new Transaction(line.split(" ").map(_.toInt).toList)
    }
    val T = transaction_buffer.toList

    val results = IterativeLCMmax.run(T, minsup)

    results.map(r => s"${r.toString()}").foreach{println}
  }
}
