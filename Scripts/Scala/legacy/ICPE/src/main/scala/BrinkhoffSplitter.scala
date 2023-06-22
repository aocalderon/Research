import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}

object BrinkhoffSplitter {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private var timer = clocktime
  private var stage = ""
  private var load  = 0L 

  case class BRecord(id: String, x: String, y: String, t: Int){
    override def toString: String = {
      s"${id}\t${x}\t${y}\t${t}\n"
    }
  }

  def clocktime = System.currentTimeMillis()

  def log(status: String = "START"): Unit ={
    logger.info("BS|%-50s|%6.2f|%6d|%s".format(stage, (clocktime - timer) / 1000.0, load, status))
  }
  def start(stage: String) = {
    this.stage = stage
    timer = clocktime
    log()
  }
  def end(load: Long = 0L) = {
    this.load = load
    log("END")
  }

  def main(args: Array[String]){
    val params = new BrinkhoffSplitterConf(args)
    val input  = params.input()
    val output = params.output()
    val tstart  = params.start()
    val tend    = params.end()
    val tag    = params.tag()

    // Starting session...
    start("Starting session")
    val spark = SparkSession.builder()
      .config("spark.default.parallelism", 3 * 10 * 12)
      .config("spark.scheduler.mode", "FAIR")
      //.config("spark.cores.max", cores * executors)
      //.config("spark.executor.cores", cores)
      //.master(master)
      .appName("BrinkhoffSplitter")
      .getOrCreate()
    import spark.implicits._
    end()

    // Reading Brinkhoff data...
    start("Reading Brinkhoff data")
    val data = spark.read.option("delimiter", "\t").csv(input)
      .map{ r =>
        val id = r.getString(0)
        val x  = r.getString(1)
        val y  = r.getString(2)
        val t  = r.getString(3).toInt
        BRecord(id, x, y, t)
      }
      .cache()
    val nData = data.count()
    end()

    // Filtering interval...
    start("Filtering interval")
    val sample = data.filter(r => r.t >= tstart && r.t < tend).cache()
    val nSample = sample.count()
    val index = sample.map(_.t).distinct()
      .collect().sorted.map(i => s"${i}\n")
    val nIndex = index.size
    val f = new java.io.PrintWriter(s"${output}${tag}_index.tsv")
    f.write(index.mkString(""))
    f.close()
    end(nSample)

    // Saving partitions...
    start("Saving partitions")
    val partitioner = new TimeInstantPartitioner(nIndex)
    val sampleRDD = sample.rdd.map(r => (r.t, r)).partitionBy(partitioner).map(_._2).toDS()
    sampleRDD.foreachPartition { records =>
      val partitionId = org.apache.spark.TaskContext.getPartitionId
      val filename = s"${output}${tag}_${partitionId}.tsv"
      val f = new java.io.PrintWriter(filename)
      f.write(records.map(_.toString()).toList.mkString(""))
      f.close()
      logger.info(s"Saving $filename")
    }
    end()

    // Closing session...
    start("Closing session")
    spark.close()
    end()
  }
}

import org.apache.spark.Partitioner
class TimeInstantPartitioner(override val numPartitions: Int) extends Partitioner {
  override def  getPartition(key: Any): Int = {
    key.asInstanceOf[Int] % numPartitions
  }
  override def equals(other: Any): Boolean = {
    other match {
      case obj: TimeInstantPartitioner => obj.numPartitions == numPartitions
      case _ => false
    }
  }
}

class BrinkhoffSplitterConf(args: Seq[String]) extends ScallopConf(args) {
  val input:  ScallopOption[String]  = opt[String]  (default = Some(""))
  val output: ScallopOption[String]  = opt[String]  (default = Some(""))
  val tag:    ScallopOption[String]  = opt[String]  (default = Some("B"))
  val start:  ScallopOption[Int]     = opt[Int]     (default = Some(0))
  val end:    ScallopOption[Int]     = opt[Int]     (default = Some(10))

  verify()
}
