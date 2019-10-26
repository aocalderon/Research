import org.rogach.scallop.{ScallopConf, ScallopOption}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable.SynchronizedQueue
import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import scala.collection.mutable.ArrayBuffer
import com.vividsolutions.jts.geom.Envelope

object FlockEnumerator1 {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class TDisk(t: Int, disk: Disk)
  case class Partition(o: Int, t: Int, d: Long, neighbours: List[Int]){
    override def toString: String = s"($o, $t): {${neighbours.mkString(" ")}}"
  }

  def getFullBoundary(disks: RDD[Disk]): Envelope = {
    val maxX = disks.map(_.x).max()
    val minX = disks.map(_.x).min()
    val maxY = disks.map(_.y).max()
    val minY = disks.map(_.y).min()
    new Envelope(minX, maxX, minY, maxY)
  }

  def main(args: Array[String]) {
    val params = new FE1Conf(args)
    val input = params.input()
    val tag = params.tag()
    val separator = params.sep()
    val extension = params.ext()
    val rate = params.rate()
    val i = params.i()
    val n = params.n()
    val delta = params.delta()
    val mu = params.mu()
    val interval = params.interval()
    val width = params.width()
    val speed = params.speed()
    val debug = params.debug()

    // Creating the session...
    val spark = SparkSession.builder()
      .appName("QueueStreamer")
      .getOrCreate()
    import spark.implicits._

    // Setting the queue...
    val ssc = new StreamingContext(spark.sparkContext, Seconds(interval))
    val rddQueue = new SynchronizedQueue[RDD[TDisk]]()
    val stream = ssc.queueStream(rddQueue)
      .window(Seconds(delta), Seconds(interval))
      .map(d => (d.t, d.disk))

    // Working with the batch window...
    stream.foreachRDD { (disks: RDD[(Int, Disk)], ts: Time) =>
      println(ts.toString())
      println(disks.count())
      val boundary = getFullBoundary(disks.map(_._2))
      val timestamps = disks.map(_._1).distinct().collect().sorted
      val t_0 = timestamps.head
      println(timestamps.mkString(" "))

      for(t_i <- timestamps){
        println(s"Timestamp: $t_i")
        val T_i = disks.filter(_._1 == t_i).map(_._2).cache
        val expansion = (t_i - t_0) * speed
        val data = DiskIndex(T_i, boundary, width, expansion)
        val I_i = data.index().cache
        
        if(debug){
          val f = new java.io.PrintWriter(s"/tmp/T_${t_i}.wkt")
          f.write(
            I_i.mapPartitionsWithIndex{ case(i, disks) =>
              disks.map(d => s"${i}\t${d.toWKT}\n")
            }.collect().mkString("")
          )
          f.close()
          println(s"Index's size: ${I_i.count()}")
          val grids = data.getGrids()
          val g = new java.io.PrintWriter(s"/tmp/I_${t_i}.wkt")
          g.write(grids.map(g => s"${g._1}\t${g._2.toText()}\t${g._3.toText()}\n").mkString(""))
          g.close()
        }
      }
    }

    // Let's start the stream...
    ssc.start()
    
    // Let's feed the stream...
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

    // Closing the session...
    ssc.stop()
    spark.close()
  }
}

class FE1Conf(args: Seq[String]) extends ScallopConf(args) {
  val input: ScallopOption[String] = opt[String] (default = Some("/home/acald013/Datasets/ICPE/Demo/in/"))
  val tag:      ScallopOption[String] = opt[String] (default = Some("LA"))
  val sep:      ScallopOption[String] = opt[String] (default = Some("_"))
  val ext:      ScallopOption[String] = opt[String] (default = Some("tsv"))
  val rate:     ScallopOption[Long]   = opt[Long]   (default = Some(1000L))
  val n:        ScallopOption[Int]    = opt[Int]    (default = Some(5))
  val i:        ScallopOption[Int]    = opt[Int]    (default = Some(0))
  val delta:    ScallopOption[Int]    = opt[Int]    (default = Some(4))
  val mu:       ScallopOption[Int]    = opt[Int]    (default = Some(2))
  val interval: ScallopOption[Int]    = opt[Int]    (default = Some(1))
  val debug:    ScallopOption[Boolean]= opt[Boolean](default = Some(false))

  val width:    ScallopOption[Double] = opt[Double]    (default = Some(500.0))
  val speed:    ScallopOption[Double] = opt[Double]    (default = Some(10.0))

  verify()
}
