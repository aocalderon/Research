import org.rogach.scallop.{ScallopConf, ScallopOption}
import scala.collection.mutable.Queue
import scala.io.Source
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

object QueueStreamTester {
  case class TDisk(t: Int, disk: Disk)

  def main(args: Array[String]) {
    val params = new QueueStreamerConf(args)
    val input = params.input()
    val tag = params.tag()
    val separator = params.sep()
    val extension = params.ext()
    val rate = params.rate()
    val i = params.i()
    val n = params.n()

    val sparkConf = new SparkConf().setAppName("QueueStream")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    val rddQueue = new Queue[RDD[TDisk]]()

    val inputStream = ssc.queueStream(rddQueue)
    
    val mappedStream = inputStream.window(Seconds(4), Seconds(1))
      .map(d => s"POINT(${d.disk.x} ${d.disk.y})\t${d.disk.pids}\t${d.t}")
    mappedStream.print(15)
    ssc.start()

    // Read and push some RDDs into the queue...
    for (t <- i to n) {
      rddQueue.synchronized {
        val filename = s"${input}${tag}${separator}${t}.${extension}"
        val in = Source.fromFile(filename)
        val disks = in.getLines.map{ line => 
          val arr = line.split("\t")
          val disk = Disk(arr(1).toDouble, arr(2).toDouble, arr(3).split(" ").map(_.toInt).toSet)

          TDisk(t, disk)
        }.toList
        rddQueue += ssc.sparkContext.parallelize(disks)
      }
      Thread.sleep(rate)
    }
    ssc.stop()
  }
}

class QueueStreamerConf(args: Seq[String]) extends ScallopConf(args) {
  val input: ScallopOption[String] = opt[String] (default = Some("/home/acald013/Datasets/ICPE/Demo/in/"))
  val tag:   ScallopOption[String] = opt[String] (default = Some("LA"))
  val sep:   ScallopOption[String] = opt[String] (default = Some("_"))
  val ext:   ScallopOption[String] = opt[String] (default = Some("tsv"))
  val start: ScallopOption[Long]   = opt[Long]   (default = Some(0L))
  val rate:  ScallopOption[Long]   = opt[Long]   (default = Some(1000L))
  val n:     ScallopOption[Int]    = opt[Int]    (default = Some(5))
  val i:     ScallopOption[Int]    = opt[Int]    (default = Some(0))
  
  verify()
}
