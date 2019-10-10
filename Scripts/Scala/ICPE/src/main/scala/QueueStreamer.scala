import org.rogach.scallop.{ScallopConf, ScallopOption}
import scala.collection.mutable.SynchronizedQueue
import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import scala.collection.mutable.ArrayBuffer

object QueueStreamer {
  case class TDisk(t: Int, disk: Disk)
  case class Partition(o: Int, t: Int, neighbours: List[Int]){
    override def toString: String = s"($o, $t): {${neighbours.mkString(" ")}}"
  }

  def main(args: Array[String]) {
    val params = new QSConf(args)
    val input = params.input()
    val tag = params.tag()
    val separator = params.sep()
    val extension = params.ext()
    val rate = params.rate()
    val i = params.i()
    val n = params.n()
    val delta = params.delta()
    val interval = params.interval()

    val spark = SparkSession.builder()
      .master("local[12]")
      .appName("QueueStreamer")
      .getOrCreate()
    import spark.implicits._
    val ssc = new StreamingContext(spark.sparkContext, Seconds(interval))

    val rddQueue = new SynchronizedQueue[RDD[TDisk]]()

    val stream = ssc.queueStream(rddQueue)
      .window(Seconds(delta), Seconds(interval))
      .map(d => (d.t, d.disk))
    stream.foreachRDD { (disks: RDD[(Int, Disk)], ts: Time) =>
      println(ts.toString())

      var partitions = disks.flatMap{ disk =>
        val t = disk._1
        val objects = disk._2.pids.toList.sorted
        objects.map{ o =>
          Partition(o, t, objects.filter(_ > o))
        }
      }.filter(!_.neighbours.isEmpty)
      
      val t = partitions.map(_.t).min
      val index = partitions.map(_.o).distinct().collect().sorted

      val partitioner = new IdPartitioner(index.size, index)
      partitions = partitions.map(p => (p.o, p)).partitionBy(partitioner).map(_._2)

      partitions.mapPartitionsWithIndex{ case (index, partition) =>
        val P = partition.toList.groupBy(_.o).head
        val o  = P._1
        val Pj = P._2
        val Pt = Pj.filter(_.t == t).flatMap(_.neighbours)
        Pt.map{ obj =>
          val B = Array.ofDim[Int](delta)
          Pj.filter(_.neighbours contains obj).foreach{ p =>
            val i = p.t - t
            B(i) = 1
          }
          (obj, B)
        }.filter(b => b._2.reduce(_ + _) == delta)
          .map{ case (obj, b) =>
            (o, s"[$index] P${t}(o${o}): o($obj) ${b.mkString("  ")}")
          }
          .toIterator
      }.collect().sortBy(_._1).map(_._2).foreach(println)

      // Output results...
      val T = (t until (t + delta)).map(s => "%-20s".format(s)).toList.mkString("")
      println(s"                  $T")
      partitions.mapPartitions{ pids =>
          pids.toList.groupBy(_.o).map(_._2).toIterator
        }
        .mapPartitionsWithIndex{ case (i, p) =>
          val B = Array.ofDim[String](delta).map(x => "{}")
          val part = p.next()
          part.foreach { pid =>
            val i = pid.t - t
            B(i) = "{%s}".format(pid.neighbours.mkString(","))
          }
          val Pt = B.map(s => "%-20s".format(s)).mkString("")
          List(s"Subtask ${i} for o${i+1}: ${Pt}").toIterator
        }.collect().sorted.foreach(println)
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
    ssc.stop()
    spark.close()
  }

  import org.apache.spark.Partitioner
  import scala.collection.Searching._
  class IdPartitioner(override val numPartitions: Int, index: Array[Int]) extends Partitioner {
    override def  getPartition(key: Any): Int = {
      val i = key.asInstanceOf[Int]

      index.search(i) match {
        case f: Found => f.foundIndex
        case _ => -1
      }
    }
    override def equals(other: Any): Boolean = {
      other match {
        case obj: IdPartitioner => obj.numPartitions == numPartitions
        case _ => false
      }
    }
  }
}

class QSConf(args: Seq[String]) extends ScallopConf(args) {
  val input: ScallopOption[String] = opt[String] (default = Some("/home/acald013/Datasets/ICPE/Demo/in/"))
  val tag:      ScallopOption[String] = opt[String] (default = Some("LA"))
  val sep:      ScallopOption[String] = opt[String] (default = Some("_"))
  val ext:      ScallopOption[String] = opt[String] (default = Some("tsv"))
  val rate:     ScallopOption[Long]   = opt[Long]   (default = Some(1000L))
  val n:        ScallopOption[Int]    = opt[Int]    (default = Some(5))
  val i:        ScallopOption[Int]    = opt[Int]    (default = Some(0))
  val delta:    ScallopOption[Int]    = opt[Int]    (default = Some(4))
  val interval: ScallopOption[Int]    = opt[Int]    (default = Some(1))
  
  verify()
}
