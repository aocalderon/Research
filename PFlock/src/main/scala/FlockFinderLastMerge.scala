import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object FlockFinderLastMerge {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class ST_Point(id: Int, x: Double, y: Double, t: Int)
  case class Flock(start: Int, end: Int, ids: String, lon: Double = 0.0, lat: Double = 0.0)
  case class Disk(t: Int, ids: String, x: Double, y: Double)
  case class ATuple(t: Int, ids: String, x: Double, y: Double, t2: Int, ids2: String, x2: Double, y2: Double)

  def mergeLast(conf: Conf): Unit = {
    // Setting paramaters...
    var timer = System.currentTimeMillis()
    val epsilon: Double = conf.epsilon()
    val speed: Double = conf.speed()
    val mu: Int = conf.mu()
    val delta: Int = conf.delta()
    val decimals: Int = conf.decimals()
    val tstart: Int = conf.tstart()
    val tend: Int = conf.tend()
    val partitions: Int = conf.partitions()
    val cores: Int = conf.cores()
    val printIntermediate: Boolean = conf.debug()
    val printFlocks: Boolean = conf.print()
    val separator: String = conf.separator()
    val logs: String = conf.logs()
    val path: String = conf.path()
    val dataset: String = conf.dataset()
    val extension: String = conf.extension()
    var master: String = conf.master()
    if (conf.cores() == 1) {
      master = "local"
    }
    val home: String = scala.util.Properties.envOrElse(conf.home(), "/home/and/Documents/PhD/Research/")
    val point_schema = ScalaReflection
      .schemaFor[ST_Point]
      .dataType
      .asInstanceOf[StructType]
    val distanceBetweenTimestamps = speed * delta
    var msg = "Setting paramaters..."
    logger.warn("%-70s [%.3fs]".format(msg, (System.currentTimeMillis() - timer)/1000.0))
    
    // Starting a session...
    timer = System.currentTimeMillis()
    val simba = SimbaSession.builder()
      .master(master)
      .appName("FlockFinder")
      .config("simba.index.partitions", partitions)
      .config("spark.cores.max", cores)
      .getOrCreate()
    simba.sparkContext.setLogLevel(logs)
    import simba.implicits._
    import simba.simbaImplicits._
    msg = "Starting current session..."
    logger.warn("%-70s [%.3fs]".format(msg, (System.currentTimeMillis() - timer)/1000.0))

    // Reading data...
    timer = System.currentTimeMillis()
    val filename = "%s%s%s.%s".format(home, path, dataset, extension)
    val pointset = simba.read
      .option("header", "false")
      .option("sep", separator)
      .schema(point_schema)
      .csv(filename)
      .as[ST_Point]
      .filter(datapoint => datapoint.t >= tstart && datapoint.t <= tend)
      .cache()
    val nPointset = pointset.count()
    msg = "Reading %s...".format(dataset)
    logger.warn("%-70s [%.3fs] [%d records]".format(msg, (System.currentTimeMillis() - timer)/1000.0, nPointset))

    // Extracting timestamps...
    timer = System.currentTimeMillis()
    val timestamps = pointset
        .map(datapoint => datapoint.t)
        .distinct
        .sort("value")
        .collect
        .toList
    val nTimestamps = timestamps.length
    msg = "Extracting timestamps..."
    logger.warn("%-70s [%.3fs] [%d timestamps]".format(msg, (System.currentTimeMillis() - timer)/1000.0, nTimestamps))

    // Running experiments with different values of epsilon, mu and delta...
    logger.warn("\n\n*** Epsilon=%.1f, Mu=%d and Delta=%d ***\n".format(epsilon, mu, delta))

    // Storing final set of flocks...
    var FinalFlocks: Dataset[Flock] = simba.sparkContext.emptyRDD[Flock].toDS()
    var nFinalFlocks: Long = 0

    /***************************************
    *     Starting Flock Evaluation...     *
    ***************************************/
    // Initializing initial variables...
    var start = 0
    val disks_map = collection.mutable.Map[Int, Dataset[Disk]]()
    var D: Dataset[Disk] = simba.sparkContext.emptyRDD[Disk].toDS()  // Set of disks on current timestamps' slice...
    var nD: Long = 0
    val timestamps_in_D: mutable.Queue[Int] = new mutable.Queue[Int]() // Set of timestamps in the current slice...

    // For each new time instance t_i...
    while(start + delta <= timestamps.length){
      logger.warn("\n\nRunning iteration with start=%d, delta=%d and timestamps.length=%d...\n".format(start, delta, timestamps.length))
      // Updating current timestamps' slice ...
      timer = System.currentTimeMillis()
      var timestamps_slice = timestamps.slice(start, start + delta)
      val nTimestamps_slice = timestamps_slice.length
      msg = "Updating current timestamps' slice..."
      logger.warn("%-70s [%.3fs] [%d timestamps]".format(msg, (System.currentTimeMillis() - timer)/1000.0, nTimestamps_slice))

      if(printIntermediate) logger.warn("Timestamps' slice: %s".format(timestamps_slice.mkString(" ")))

      // Adding maximal disks to current timestamps' slice...
      timer = System.currentTimeMillis()
      for(t <- timestamps_slice){
        if(!timestamps_in_D.contains(t)){
          logger.warn("\n\n")
          val timer1 = System.currentTimeMillis()
          val points = pointset
            .filter(datapoint => datapoint.t == t)
          val currentPoints = points.map{ datapoint =>
              "%d\t%f\t%f".format(datapoint.id, datapoint.x, datapoint.y)
            }
            .rdd
            .cache()
          val nCurrentPoints = currentPoints.count()
          msg = "Reported location for trajectories in time %d...".format(t)
          logger.warn("%-70s [%.3fs] [%d points]".format(msg, (System.currentTimeMillis() - timer1)/1000.0, nCurrentPoints))

          // Set of disks for t_i...
          val timer2 = System.currentTimeMillis()
          var C: Dataset[Disk] = MaximalFinderExpansion
            .run(currentPoints, simba, conf)
            .map{ m =>
              val disk = m.split(";")
              val x = disk(0).toDouble
              val y = disk(1).toDouble
              val ids = disk(2)
              Disk(t, ids, x, y)
            }
            .toDS()
            .index(RTreeType, "d%dRT".format(t), Array("x", "y"))
            .cache()
          val nC = C.count()
          logger.warn("\n")
          msg = "Set of disks for timestamp %d...".format(t)
          logger.warn("%-70s [%.3fs] [%d disks]".format(msg, (System.currentTimeMillis() - timer2)/1000.0, nC))

          // Adding current set of disks to D...
          val timer3 = System.currentTimeMillis()
          disks_map += (t -> C)
          msg = "Adding %d new disks to D...".format(nC)
          logger.warn("%-70s [%.3fs] [%d total disks]".format(msg, (System.currentTimeMillis() - timer3)/1000.0, nD))

          // Adding t to timestamps in D...
          val timer4 = System.currentTimeMillis()
          timestamps_in_D.enqueue(t)
          val nTimestamps_in_D = timestamps_in_D.length
          msg = "Adding t to timestamps in D..."
          logger.warn("%-70s [%.3fs] [%d times]".format(msg, (System.currentTimeMillis() - timer4)/1000.0, nTimestamps_in_D))
        }
      }
      msg = "Adding maximal disks to current timestamps' slice..."
      logger.warn("%-70s [%.3fs] [%d disks]".format(msg, (System.currentTimeMillis() - timer)/1000.0, nD))

      // Getting disks coordinates...
      timer = System.currentTimeMillis()
      val pointset_subset = pointset.filter(p => p.t >= start && p.t <= start + delta - 1)
      D = getCoordinates(D, pointset_subset, simba, decimals).cache()
      nD = D.count()
      msg = "Getting disks coordinates..."
      logger.warn("%-70s [%.3fs] [%d disks located]".format(msg, (System.currentTimeMillis() - timer)/1000.0, nD))

      // Reordering current timestamps' slice ...
      timer = System.currentTimeMillis()
      timestamps_slice = reorder(timestamps_slice)
      msg = "Reordering current timestamps' slice..."
      logger.warn("%-70s [%.3fs] [%d timestamps]".format(msg, (System.currentTimeMillis() - timer)/1000.0, nTimestamps_slice))

      if(printIntermediate){
        logger.warn("Timestamps' slice order: %s".format(timestamps_slice.mkString(" ")))
      }

      // Indexing D_0...
      if(printIntermediate) logger.warn("Indexing D_%d...".format(timestamps_slice.head))
      val timer1 = System.currentTimeMillis()
      var D_prime = D.filter(d => d.t == timestamps_slice.head).cache()
      D_prime.index(RTreeType, "d_primeRT", Array("x", "y"))
      var nD_prime = D_prime.count()
      msg = "Indexing D_%d...".format(timestamps_slice.head)
      logger.warn("%-70s [%.3fs] [%d disks]".format(msg, (System.currentTimeMillis() - timer1)/1000.0, nD_prime))

      // Processing remaining timestamps in slice...
      for(i <- timestamps_slice.indices.drop(1)){
        logger.warn("\n\n")
        // Indexing D_i...
        if(printIntermediate) logger.warn("Indexing D_%d...".format(i))
        val timer2 = System.currentTimeMillis()
        val D_i = D.filter(d => d.t == timestamps_slice(i)).cache()
        D_i.index(RTreeType, "d_%dRT".format(i), Array("x", "y"))
        val nD_i = D_i.count()
        msg = "Indexing D_%d...".format(i)
        logger.warn("%-70s [%.3fs] [%d disks]".format(msg, (System.currentTimeMillis() - timer2)/1000.0, nD_i))

        if(printIntermediate)
          logger.warn("Distance Join and filtering phase using distance=%.3f mu=%d...".format(distanceBetweenTimestamps, mu))

        // Distance Join and filtering phase...
        val timer3 = System.currentTimeMillis()
        val U = D_i.distanceJoin(D_prime.toDF("t2", "ids2", "x2", "y2"), Array("x", "y"), Array("x2", "y2"), distanceBetweenTimestamps)
          .as[ATuple]
          .map{ tuple =>
            val ids1 = tuple.ids.split(" ").map(_.toLong)
            val ids2 = tuple.ids2.split(" ").map(_.toLong)
            val ids = ids1.intersect(ids2)

            (Disk(0, ids.mkString(" "), tuple.x, tuple.y), ids.length)
          }
          .filter(_._2 >= mu)
          .map(_._1)
          .cache()
        val nU = U.count()
        msg = "Distance Join and filtering phase between timestamps %d and %d...".format(i - 1, i)
        logger.warn("%-70s [%.3fs] [%d disks]".format(msg, (System.currentTimeMillis() - timer3) / 1000.0, nU))

        // Indexing D_prime...
        if(printIntermediate) logger.warn("Indexing D_prime...")
        val timer1 = System.currentTimeMillis()
        D_prime.dropIndex()
        D_prime = U.index(RTreeType, "d_primeRT", Array("x", "y")).cache()
        nD_prime = D_prime.count()
        msg = "Indexing D_prime..."
        logger.warn("%-70s [%.3fs] [%d found flocks]".format(msg, (System.currentTimeMillis() - timer1)/1000.0, nD_prime))
      }

      if(printIntermediate) {
        logger.warn("\n\nFound %d flocks in timestamp %d...\n".format(D_prime.count(), start + delta - 1))
        D_prime.show()
      }

      // Reporting flocks...
      FinalFlocks = FinalFlocks.union{
        D_prime.map{ disk =>
          Flock(start, start + delta - 1, disk.ids)
        }
      }.cache()
      nFinalFlocks = FinalFlocks.count()

      // Reporting summary...
      logger.warn("\n\nPFLOCK\t%d\t%s\t%.1f\t%d\t%d\t%d\n"
        .format(start, timestamps_slice.mkString(" "), epsilon, mu, delta, nFinalFlocks))

      // Updating next slice...
      timer = System.currentTimeMillis()
      D = D.filter(d => d.t != start).cache()
      nD = D.count()
      timestamps_in_D.dequeue()
      start = start + 1
      msg = "Updating next slice..."
      logger.warn("%-70s [%.3fs] [%d timestamps]".format(msg, (System.currentTimeMillis() - timer)/1000.0, nD))
    }

    if(printFlocks){ // Reporting final set of flocks...
      val finalFlocks = FinalFlocks.collect()
      val nFinalFlocks = finalFlocks.length
      val flocksReport = finalFlocks
        .map{ f =>
          "%d, %d, %s\n".format(f.start, f.end, f.ids)
        }
        .mkString("")
      logger.warn("\n\n%s\n".format(flocksReport))
      logger.warn("\n\nFinal flocks: %d\n".format(nFinalFlocks))
    }


    // Closing all...
    logger.warn("Closing app...")
    simba.close()
  }

  def reorder(list: List[Int]): List[Int] = {
    val queue = new mutable.Queue[(Int, Int)]()
    val result = new ListBuffer[Int]()
    var lo = 0
    var hi = list.length - 1

    result += lo
    result += hi
    queue.enqueue((lo, hi))

    while(queue.nonEmpty){
      val pair = queue.dequeue()
      val lo = pair._1
      val hi = pair._2
      if(lo + 1 == hi){
      } else {
        val mi = lo + (hi - lo) / 2
        result += mi
        queue.enqueue((lo, mi))
        queue.enqueue((mi, hi))
      }
    }

    result.toList.map(i => list(i))
  }

  /* Extract coordinates from a set of point IDs */
  def getCoordinates(C: Dataset[Disk], points: Dataset[ST_Point], simba: SimbaSession, decimals: Int = 3): Dataset[Disk] = {
    import simba.implicits._
    
    val C_prime = C.map(c => (c.t, c.ids, c.ids.split(" ").map(_.toLong)))
      .toDF("t" , "ids", "idsList")
      .withColumn("id", explode($"idsList"))
      .select("t", "ids", "id")

    C_prime.join(points, Array("t", "id"))
      .select("t", "ids", "x", "y")
      .map(p => ("%d:%s".format(p.getInt(0), p.getString(1)), (p.getDouble(2), p.getDouble(3))))
      .rdd
      .reduceByKey( (a, b) =>
        ( a._1 + b._1, a._2 + b._2 )
      )
      .map{ f =>
        val key = f._1.split(":")
        val t = key(0).toInt
        val ids = key(1)
        val n = ids.split(" ").length
        val x = BigDecimal(f._2._1 / n).setScale(decimals, BigDecimal.RoundingMode.HALF_UP).toDouble
        val y = BigDecimal(f._2._2 / n).setScale(decimals, BigDecimal.RoundingMode.HALF_UP).toDouble
        Disk(t, ids, x, y)
      }
      .toDS()
  }
  
  
  def saveStringArray(array: Array[String], tag: String, conf: Conf): Unit = {
    val path = s"/tmp/"
    val filename = s"${conf.dataset()}_E${conf.epsilon()}_M${conf.mu()}_D${conf.delta()}"
    new java.io.PrintWriter("%s%s_%s.txt".format(path, filename, tag)) {
      write(array.mkString("\n"))
      close()
    }
  }
  
  def pruneFlocks(U: RDD[Flock], partitions: Int): RDD[Flock] = {
    var U_prime = U.mapPartitions{ records =>
      var flocks = new ListBuffer[(Flock, Boolean)]()
      for(record <- records){
        flocks += Tuple2(record, true)
      }
      for(i <- flocks.indices){
        for(j <- flocks.indices){
          if(i != j & flocks(i)._2){
            val ids1 = flocks(i)._1.ids.split(" ").map(_.toLong)
            val ids2 = flocks(j)._1.ids.split(" ").map(_.toLong)
            if(flocks(j)._2 & ids1.forall(ids2.contains)){
              val s1 = flocks(i)._1.start
              val s2 = flocks(j)._1.start
              val e1 = flocks(i)._1.end
              val e2 = flocks(j)._1.end
              if(s1 == s2 & e1 == e2){
                flocks(i) = Tuple2(flocks(i)._1, false)
              }
            }
            if(flocks(i)._2 & ids2.forall(ids1.contains)){
              val s1 = flocks(i)._1.start
              val s2 = flocks(j)._1.start
              val e1 = flocks(i)._1.end
              val e2 = flocks(j)._1.end
              if(s1 == s2 & e1 == e2){
                flocks(j) = Tuple2(flocks(j)._1, false)
              }
            }
          }
        }
      }
      flocks.filter(_._2).map(_._1).toIterator
    }.cache()

    U_prime = U_prime.repartition(1).
      mapPartitions{ records =>
        var flocks = new ListBuffer[(Flock, Boolean)]()
        for(record <- records){
          flocks += Tuple2(record, true)
        }
        for(i <- flocks.indices){
          for(j <- flocks.indices){
            if(i != j & flocks(i)._2){
            val ids1 = flocks(i)._1.ids.split(" ").map(_.toLong)
            val ids2 = flocks(j)._1.ids.split(" ").map(_.toLong)
              if(flocks(j)._2 & ids1.forall(ids2.contains)){
                val s1 = flocks(i)._1.start
                val s2 = flocks(j)._1.start
                val e1 = flocks(i)._1.end
                val e2 = flocks(j)._1.end
                if(s1 == s2 & e1 == e2){
                  flocks(i) = Tuple2(flocks(i)._1, false)
                }
              }
              if(flocks(i)._2 & ids2.forall(ids1.contains)){
                val s1 = flocks(i)._1.start
                val s2 = flocks(j)._1.start
                val e1 = flocks(i)._1.end
                val e2 = flocks(j)._1.end
                if(s1 == s2 & e1 == e2){
                  flocks(j) = Tuple2(flocks(j)._1, false)
                }
              }
            }
          }
        }
        flocks.filter(_._2).map(_._1).toIterator
      }.repartition(partitions).cache()

    U_prime
  }

  def Flocks2String(flocks: RDD[Flock]): String = {
    val n = flocks.count()
    val info = flocks.map{ f => 
      "\n%d,%d,%s,%.2f,%.2f".format(f.start, f.end, f.ids, f.lon, f.lat)
    }.collect.mkString("")
    
    "# of flocks: %d\n%s".format(n,info)
  }

  def saveFlocks(flocks: RDD[Flock], filename: String): Unit = {
    new java.io.PrintWriter(filename) {
      write(
        flocks.map{ f => 
          "%d, %d, %s, %.3f, %.3f\n".format(f.start, f.end, f.ids, f.lon, f.lat)
        }.collect.mkString("")
      )
      close()
    }
  }

  def main(args: Array[String]): Unit = {
    logger.info("Starting app...")
    val conf = new Conf(args)
    FlockFinderLastMerge.mergeLast(conf)
  }
}
