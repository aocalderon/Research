import org.rogach.scallop.{ScallopConf, ScallopOption}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable.SynchronizedQueue
import scala.io.Source
import scala.collection.mutable.{HashMap, HashSet}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.enums.{GridType}
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, PointRDD, CircleRDD}
import scala.collection.mutable.ArrayBuffer
import com.vividsolutions.jts.geom.{Envelope, Coordinate, Point}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Geometry}
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
      val prevs = (0 to i).map{ j =>
        flocksMap.get(keys(j)).get.toList.map(_.getItemset)
      }.reduce(_ ++ _)
      val currs = flocksMap.get(keys(i + 1)).get.filterNot{ f =>
        val flock = f.getItemset
        prevs.map(prev => flock.subsetOf(prev)).reduce(_ || _)
      }
      flocksMap += (keys(i + 1) -> currs)
    }
    flocksMap.values.flatMap(_.toList).toArray
  }

  def getFlocksFromGeom(g: Geometry): Flock = {
    val farr   = g.getUserData.toString().split("\t")
    val items  = farr(0).split(" ").map(_.toInt).toArray
    val start  = farr(1).toInt
    val end    = farr(2).toInt
    val center = g.getCentroid

    val flock = Flock(items, start, end)
    flock.center = center
    flock
  }

  def getRedundants(flocks: RDD[Flock], epsilon: Double, spark: SparkSession, params: FFConf, debug: Boolean = false): RDD[Flock] = {
    if(flocks.isEmpty()){
      flocks
    } else {
      import spark.implicits._
      val points = flocks.map{ flock =>
        val point = flock.center
        point.setUserData(s"${flock.items.sorted.mkString(" ")}\t${flock.start}\t${flock.end}")
        point
      }.cache
      val nPoints = points.count().toInt
      val pointsRDD = new SpatialRDD[Point]()
      pointsRDD.setRawSpatialRDD(points)
      pointsRDD.analyze()
      var nPartitions = params.ffpartitions()
      if(nPartitions > nPoints / 2){
        nPartitions =  4
      }
      pointsRDD.spatialPartitioning(GridType.KDBTREE, nPartitions)
      //pointsRDD.buildIndex(IndexType.QUADTREE, true)
      val bufferRDD = new CircleRDD(pointsRDD, epsilon)
      bufferRDD.spatialPartitioning(pointsRDD.getPartitioner)
      val F = JoinQuery.DistanceJoinQuery(pointsRDD, bufferRDD, false, false)
      val flocks_prime = F.rdd.map{ case (flock: Point, flocks: java.util.HashSet[Point]) =>
        (flock, flocks.asScala.toList)
      }

      val f = flocks_prime.flatMap{ entry =>
        val flock1  = getFlocksFromGeom(entry._1)
        val flocks = entry._2.map(getFlocksFromGeom)
        flocks.map{ flock2 => (flock1, flock2) }
          .filter(flocks => flocks._1.size < flocks._2.size)
          .map{flocks =>
            val flock1 = flocks._1
            val flock2 = flocks._2
            val subset = flock1.items.toSet.subsetOf(flock2.items.toSet)
            (flock1, flock2, subset)
          }
      }

      if(debug){
        f.map(f => (f._1.toString(), f._2.toString(), f._3)).toDS().show(100, false)
      }

      f.filter(_._3).map(_._1)
    }
  }

  def enumerate(disks: RDD[TDisk], spark: SparkSession, params: FFConf): Unit = {
    import spark.implicits._

    val epsilon = params.epsilon()
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
    val t_n = timestamps.take(1).head
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
            val transactions = B.toList.map{ tdisk =>
              tdisk.disk.getItems.map(p => new Integer(p)).asJava
            }.asJava

            val tidTest = 1380563
            if(tid == tidTest){
              B.toList.sortBy(b => (b.t,  b.disk.getItems.head))
                .map(b => s"${b.t}\t${b.disk.getItems.mkString(" ")}")
                .foreach{println}
              val D = B.toList
              for(t <- timestamps){
                val D_prime = D.filter(d => t == d.t).sortBy(_.disk.getItems.head)
                  .map(d => s"${d.t}\t${d.disk.getItems.mkString(" ")}\t${d.disk.toCircle(epsilon/2.0).toText()}\n")
                val filename = s"/tmp/windowTest_${t}.wkt"
                val f = new java.io.PrintWriter(filename)
                f.write(D_prime.mkString(""))
                f.close()
                logger.info(s"Saved $filename [${D_prime.size} records].")
              }
            }

            val fpmax = new AlgoFPMax()
            val maximals = fpmax.runAlgorithm(transactions, timestamps.size) // Should be delta
              .getLevels.asScala.flatMap{ level =>
                level.asScala
              }

            if(tid == tidTest){
              maximals.sortBy(_.getItems.sorted.head)
                .map(_.getItems.mkString(" "))
                .foreach{println}
            }

            val flocks = maximals.filter(_.getItems.size + 1 >= mu)
              .map{ maximal => // Checking consecutiveness ...
                val m = maximal.getItems.toSet
                (maximal, B.par.filter(b => m.subsetOf(b.disk.getItemset)))
              }.filter{ case (maximal, tdisks) =>
                  val b = tdisks.map(_.t)
                  b.size >= delta // Keeping just those which touch delta timestamps
              }.map{ case(maximal, tdisks) =>
                  val items = (maximal.getItems ++ Array(tid)).sorted
                  val t_n = t_0 + delta - 1
                  val flock = Flock(items, t_0, t_n)
                  val last_disk = tdisks.filter(_.t == t_n).head.disk // Keeping last position of flock...
                  flock.center = geofactory.createPoint(new Coordinate(last_disk.x, last_disk.y))
                  flock 
              }

            if(tid == tidTest){
              flocks.sortBy(_.getItems.head)
                .map(_.toString())
                .foreach{println}
            }

            flocks
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
