import org.rogach.scallop.{ScallopConf, ScallopOption}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable.SynchronizedQueue
import scala.io.Source
import scala.collection.mutable.{HashMap, HashSet}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, PointRDD}
import scala.collection.mutable.ArrayBuffer
import com.vividsolutions.jts.geom.{Envelope, Coordinate, Point}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory}
import collection.JavaConverters._
import SPMF.{AlgoFPMax, Transaction}

object FlockEnumerator2 {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val model: PrecisionModel = new PrecisionModel(100000)
  private val geofactory: GeometryFactory = new GeometryFactory(model)
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

  def enumerate(disks: RDD[TDisk], spark: SparkSession, params: FFConf): Unit = {
    import spark.implicits._

    val delta = params.delta()
    val mu    = params.mu()
    val width = params.width()
    val xMin  = params.xmin()
    val yMin  = params.ymin()
    val xMax  = params.xmax()
    val yMax  = params.ymax()
    val speed = params.speed()
    val debug = params.debug()
    val save  = params.save()

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
      disksIndexed.map(i => (i._1, i._2.toString())).foreach(println)
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
            var B = HashSet[TDisk]()
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
            if(tid == 1048492){
            //  B.foreach(println)
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

  def main(args: Array[String]) {
    val params = new FFConf(args)
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
    val debug = params.debug()

    // Creating the session...
    timer = clocktime
    stage = "Starting"
    log()
    val spark = SparkSession.builder()
      .appName("FlockEnumerator")
      .getOrCreate()
    import spark.implicits._

    // Setting the queue...
    val ssc = new StreamingContext(spark.sparkContext, Seconds(interval))
    val rddQueue = new SynchronizedQueue[RDD[ST_Point]]()
    val stream = ssc.queueStream(rddQueue)
      .window(Seconds(delta * interval), Seconds(interval))
    log()

    // Working with the batch window...
    var disksMap: HashMap[Int, RDD[Disk]] = HashMap.empty[Int, RDD[Disk]] 
    stream.foreachRDD{ (points: RDD[ST_Point], instant: Time) =>
      val timestamps = points.map(_.t).distinct().collect().sorted
      logger.info(s"Timestamps in window: ${timestamps.mkString(" ")}")
      val t_0 = timestamps.head
      val t_d = timestamps.takeRight(1).head
      // Remove previous timestamps from map...
      disksMap.keys.filter(_ < t_0).foreach{ k =>
        disksMap -= k
      }
      // Add new timestamp to map...
      val p = points.filter(_.t == t_d).map{ point =>
        val p = geofactory.createPoint(new Coordinate(point.x, point.y))
        p.setUserData(s"${point.tid}\t${point.t}")
        p
      }.cache
      val nP = p.count()
      logger.info(s"Points in timestamp: $nP")
      val pointsRDD = new SpatialRDD[Point]()
      pointsRDD.setRawSpatialRDD(p)
      pointsRDD.analyze()
      val result = MF.run(spark, pointsRDD, params, t_d, s"${t_d}")
      val maximals = result._1
      val nM = result._2
      logger.info(s"Points in timestamp: $nM")
      disksMap += t_d -> maximals.map{ d =>
        val itemset = d.getUserData.toString().split(";")(0).split(" ").map(_.toInt).toSet
        Disk(d.getX, d.getY, itemset)
      }

      val tdisks = disksMap.map(t => t._2.map(d => TDisk(t._1, d))).reduce(_ union _).cache
      val nDisks = tdisks.count()

      if(debug){
        tdisks.toDS().orderBy($"t").map(_.toString()).show(nDisks.toInt, false)
      }

      enumerate(tdisks, spark, params)
    }

    // Let's start the stream...
    ssc.start()
    
    // Let's feed the stream...
    for (t <- i to n) {
      rddQueue.synchronized {
        val filename = s"${input}${tag}${separator}${t}.${extension}"

        val in = Source.fromFile(filename)
        val points = in.getLines.map{ line => 
          val arr = line.split("\t")
          ST_Point(arr(0).toInt, arr(1).toDouble, arr(2).toDouble, arr(3).toInt)
        }.toList
        rddQueue += ssc.sparkContext.parallelize(points)
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
