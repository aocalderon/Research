import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object FlockFinderMergeLast {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class ST_Point(id: Int, x: Double, y: Double, t: Int)
  case class Flock(start: Int, end: Int, ids: String, lon: Double = 0.0, lat: Double = 0.0)
  case class Disk(t: Int, ids: String, x: Double, y: Double)
  case class Tuple(t: Int, ids: String, x: Double, y: Double, t2: Int, ids2: String, x2: Double, y2: Double)

  def mergeLast(conf: Conf): Unit = {
    // Setting paramaters...
    var timer = System.currentTimeMillis()
    val epsilon: Double = conf.epsilon()
    val speed: Double = conf.speed()
    val mu: Int = conf.mu()
    val delta: Int = conf.delta()
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
    val D = collection.mutable.Map[Int, Dataset[Disk]]()
    var nD: Long = 0
    var t: Int = 0
    while(t < timestamps.length){
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
      val C: Dataset[Disk] = MaximalFinderExpansion
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
      D += (t -> C)
      nD = D.values.map(_.count()).sum
      msg = "Adding %d new disks to D...".format(nC)
      logger.warn("%-70s [%.3fs] [%d total disks]".format(msg, (System.currentTimeMillis() - timer3)/1000.0, nD))
      
      // Catching if we have to merge...
      if(printIntermediate) logger.warn("Current values for t = %d and delta - 1 = %d".format(t, delta - 1))
      if(t >= delta - 1){
        if(printIntermediate) logger.warn("D keys size: %d".format(D.keys.size))
        var slice = D.keys.toList.sorted
        slice = reorder(slice)
        var F_prime = D.remove(slice.head).get
        var nF_prime = F_prime.count()
        slice = slice.drop(1)
        if(printIntermediate) logger.warn("Slice content: %s".format(slice.mkString(" ")))
        for(i <- slice){
          val F = D(i)
          // Distance Join and filtering phase...
          val timer1 = System.currentTimeMillis()
          val U = F_prime.distanceJoin(F.toDF("t2", "ids2", "x2", "y2"), Array("x", "y"), Array("x2", "y2"), distanceBetweenTimestamps)
            .as[Tuple]
            .map{ tuple =>
              val ids1 = tuple.ids.split(" ").map(_.toLong)
              val ids2 = tuple.ids2.split(" ").map(_.toLong)
              val ids = ids1.intersect(ids2)

              (Disk(tuple.t, ids.mkString(" "), tuple.x, tuple.y), ids.length)
            }
            .filter(_._2 >= mu)
            .map(_._1)
            .cache()
          val nU = U.count()
          msg = "Distance Join and filtering phase at timestamp %d...".format(i)
          logger.warn("%-70s [%.3fs] [%d disks]".format(msg, (System.currentTimeMillis() - timer1) / 1000.0, nU))

          // Indexing intersected dataset...
          if(printIntermediate) logger.warn("Indexing intersected dataset...")
          val timer2 = System.currentTimeMillis()
          F_prime = U.index(RTreeType, "f_primeRT", Array("x", "y")).cache()
          nF_prime = F_prime.count()
          msg = "Indexing intersected dataset..."
          logger.warn("%-70s [%.3fs] [%d intersections]".format(msg, (System.currentTimeMillis() - timer2)/1000.0, nF_prime))
        }

        // Reporting flocks...
        FinalFlocks = FinalFlocks.union{
          F_prime.map{ disk =>
            Flock(disk.t, t, disk.ids)
          }
        }.cache()
        nFinalFlocks = FinalFlocks.count()

        // Reporting summary...
        logger.warn("\n\nPFLOCK\t%.1f\t%d\t%d\t%d\t%d\n".format(epsilon, mu, delta, t, nFinalFlocks))

      }

      // Going to next timestamp...
      t += 1
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
    if(list.lengthCompare(3) < 0) return list
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
    FlockFinderMergeLast.mergeLast(conf)
  }
}
