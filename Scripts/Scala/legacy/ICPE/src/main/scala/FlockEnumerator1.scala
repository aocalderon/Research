import org.rogach.scallop.{ScallopConf, ScallopOption}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable.SynchronizedQueue
import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import scala.collection.mutable.ArrayBuffer
import com.vividsolutions.jts.geom.Envelope
import collection.JavaConverters._
import SPMF.{AlgoFPMax, Transaction}

object FlockEnumerator1 {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private var stage: String = ""
  private var timer: Long = 0L
  private var counter: Int = 0

  def getFullBoundary(disks: RDD[Disk]): Envelope = {
    val maxX = disks.map(_.x).max()
    val minX = disks.map(_.x).min()
    val maxY = disks.map(_.y).max()
    val minY = disks.map(_.y).min()
    new Envelope(minX, maxX, minY, maxY)
  }

  def clocktime: Long = System.currentTimeMillis()

  def log(n: Long = 0): Unit = {
    val status = counter % 2 match {
      case 0 => "START"
      case _ => "END"
    }
    counter = counter + 1
    logger.info("PE|%-35s|%6.2f|%6d|%s".format(stage, (clocktime-timer)/1000.0, n, status))
  }

  def pruneSubsets(flocks: RDD[Flock]): Array[Flock] = {
    var flocksMap = flocks.groupBy(_.size).map(p => p._1 -> p._2).collect().toMap
    val keys = flocksMap.keys.toList.sorted.reverse
    for(i <- 0 until keys.length - 1){
      val prevs = flocksMap.get(keys(i)).get.toList.map(_.getItemset)
      val currs = flocksMap.get(keys(i + 1)).get.filterNot{ f =>
        val flock = f.getItemset
        prevs.map(prev => flock.subsetOf(prev)).reduce(_ || _)
      }
      flocksMap += (keys(i + 1) -> currs)
    }
    flocksMap.values.flatMap(_.toList).toArray
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
    val xMin  = params.xmin()
    val yMin  = params.ymin()
    val xMax  = params.xmax()
    val yMax  = params.ymax()
    val speed = params.speed()
    val debug = params.debug()
    val save  = params.save()

    // Creating the session...
    timer = clocktime
    stage = "Starting"
    log()
    val spark = SparkSession.builder()
      .appName("QueueStreamer")
      .getOrCreate()
    import spark.implicits._

    // Setting the queue...
    val ssc = new StreamingContext(spark.sparkContext, Seconds(interval))
    val rddQueue = new SynchronizedQueue[RDD[TDisk]]()
    val stream = ssc.queueStream(rddQueue)
      .window(Seconds(delta * interval), Seconds(interval))
    log()

    // Working with the batch window...
    stream.foreachRDD{ (disks: RDD[TDisk], ts: Time) =>
      val timestamps = disks.map(_.t).distinct().collect().sorted
      val t_0 = timestamps.head
      logger.info(s"Times in window: ${timestamps.mkString(" ")}")
      var nDisks = disks.count()
      logger.info(s"Disks in window: ${nDisks}")
      val boundary = params.envelope() match {
        case true => getFullBoundary(disks.map(_.disk))
        case _    => new Envelope(xMin, xMax, yMin, yMax)
      }
      if(debug){ logger.info(s"Using $boundary as envelope...")}

      // Indexing...
      timer = clocktime
      stage = "Indexing"
      log()
      val indexer = DiskPartitioner(boundary, width)
      val disksIndexed = disks.flatMap{ disk =>
        val expansion = (disk.t - t_0) * speed
        val index = indexer.indexByExpansion(disk, expansion)
        index
      }.cache
      nDisks = disksIndexed.count()
      log(nDisks)

      if(debug){
        disksIndexed.map(i => (i._1, i._2.toString())).toDF().show(nDisks.toInt, false)
      }

      // Partitioning...
      timer = clocktime
      stage = "Partitioning"
      log()
      var disksPartitioned = disksIndexed
        .partitionBy(new KeyPartitioner(indexer.getNumPartitions))
        .map(_._2).cache
      val nonEmpty = spark.sparkContext.longAccumulator("nonEmpty")
      disksPartitioned.foreachPartition{ p =>
        if(p.length > 0) nonEmpty.add(1)
      }
      disksPartitioned = disksPartitioned.coalesce(nonEmpty.value.toInt)
      nDisks = disksPartitioned.count()
      log(nDisks)

      if(debug){
        disksPartitioned.mapPartitions(partition => List(partition.size).toIterator).foreach(println)
        logger.info(s"Number of replicated disks: ${nDisks}")
        logger.info(s"Number of partitions: ${disksPartitioned.getNumPartitions}")
      }

      // Mining...
      timer = clocktime
      stage = "Mining"
      log()
      val patterns = disksPartitioned.mapPartitionsWithIndex{ case(index, partition) =>
        val part = partition.toList.groupBy(_.t).map{ t =>
          (t._1 -> t._2.sortBy(_.t))
        }.toMap
        part.keySet.exists(_ == t_0) match {
          case true => {
            val disks_0 = part.get(t_0).get.map(p => p.disk)
            val trajectories = disks_0.flatMap(_.pids).distinct.sorted

            trajectories.flatMap{ tid =>
              var B = new ArrayBuffer[TDisk]()
              timestamps.map{ t =>
                if(part.keySet.exists(_ == t)){
                  part.get(t).get.foreach{ tdisk =>
                    if(tdisk.disk.pids.contains(tid)){
                      val disk = tdisk.disk
                      B += TDisk(tdisk.t, Disk(disk.x, disk.y, disk.getItemset.filter(_ > tid)))
                    }
                  }
                }
              }

              //////////////////
              if(tid == 223196){
                B.foreach(println)
              }
              //////////////////

              val transactions = B.toList.map{ tdisk =>
                tdisk.disk.getItems.map(p => new Integer(p)).asJava
              }.asJava

              val fpmax = new AlgoFPMax()
              val maximals = fpmax.runAlgorithm(transactions, delta)
                .getLevels.asScala.flatMap{ level =>
                  level.asScala
                }

              maximals.filter(_.getItems.size + 1 >= mu).map{ maximal =>
                val items = (maximal.getItems ++ Array(tid)).sorted
                Flock(items, t_0, t_0 + delta - 1)
              }
            }.toIterator
          }
          case _ => List.empty.toIterator
        }
      }.cache
      val nPatterns = patterns.count()
      log(nPatterns)

      if(save){
        if(nPatterns > 0 && timestamps.size == delta){
          var WKT = pruneSubsets(patterns).map(_.toString()).sorted
          var filename = s"/tmp/windowFlocks_${t_0 + delta - 1}.tsv"
          var f = new java.io.PrintWriter(filename)
          f.write(WKT.mkString("\n"))
          f.write("\n")
          f.close()
          logger.info(s"Saved $filename [${WKT.size} records].")
        }
      } else {
        var flocksMap = patterns.groupBy(_.size).map(p => p._1 -> p._2).collect().toMap
        val keys = flocksMap.keys.toList.sorted.reverse
        for(i <- 0 until keys.length - 1){
          val prevs = flocksMap.get(keys(i)).get.toList.map(_.getItemset)
          val currs = flocksMap.get(keys(i + 1)).get.filterNot{ f =>
            val flock = f.getItemset
            prevs.map(prev => flock.subsetOf(prev)).reduce(_ || _)
          }
          flocksMap += (keys(i + 1) -> currs)
        }
        flocksMap.values.flatMap(_.toList).map(_.toString()).toList.sorted.foreach{println}
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
      Thread.sleep(interval * 1000L)
    }

    // Closing the session...
    timer = clocktime
    stage = "Closing"
    log()
    ssc.stop()
    spark.close()
    log()
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
  val save:     ScallopOption[Boolean]= opt[Boolean](default = Some(false))

  val width:    ScallopOption[Double] = opt[Double]    (default = Some(500.0))
  val envelope: ScallopOption[Boolean]= opt[Boolean]   (default = Some(false))
  val xmin:     ScallopOption[Double] = opt[Double]    (default = Some(0.0))
  val ymin:     ScallopOption[Double] = opt[Double]    (default = Some(0.0))
  val xmax:     ScallopOption[Double] = opt[Double]    (default = Some(3.0))
  val ymax:     ScallopOption[Double] = opt[Double]    (default = Some(3.0))

  val speed:    ScallopOption[Double] = opt[Double]    (default = Some(10.0))

  verify()
}
