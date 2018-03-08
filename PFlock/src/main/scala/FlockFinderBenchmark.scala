import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.simba.{Dataset, SimbaSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

object FlockFinderBenchmark {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class ST_Point(id: Int, x: Double, y: Double, t: Int)
  case class Flock(start: Int, end: Int, ids: String, lon: Double = 0.0, lat: Double = 0.0)

  def run(conf: Conf): Unit = {
    // Setting paramaters...
    var timer = System.currentTimeMillis()
    val epsilon: Double = conf.epsilon()
    val mu: Int = conf.mu()
    val delta: Int = conf.delta()
    val partitions: Int = conf.partitions()
    val cores: Int = conf.cores()
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
    logger.warn("Setting paramaters... [%.3fs]".format((System.currentTimeMillis() - timer)/1000.0))

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
    logger.warn("Starting current session... [%.3fs]".format((System.currentTimeMillis() - timer)/1000.0))

    // Reading data...
    timer = System.currentTimeMillis()
    val filename = "%s%s%s.%s".format(home, path, dataset, extension)
    val pointset = simba.read
      .option("header", "false")
      .option("sep", separator)
      .schema(point_schema)
      .csv(filename)
      .as[ST_Point]
      .cache()
    val nPointset = pointset.count()
    logger.warn("Reading %s... [%.3fs] [%d records]".format(dataset, (System.currentTimeMillis() - timer)/1000.0, nPointset))

    // Extracting timestamps...
    timer = System.currentTimeMillis()
    val timestamps = pointset
      .map(datapoint => datapoint.t)
      .distinct
      .sort("value")
      .collect
      .toList
    val nTimestamps = timestamps.length
    logger.warn("Extracting timestamps... [%.3fs] [%d timestamps]".format((System.currentTimeMillis() - timer)/1000.0, nTimestamps))

    // Running experiments with different values of epsilon, mu and delta...
    logger.warn("\n\n*** Epsilon=%.1f, Mu=%d and Delta=%d ==SpatialJoin== ***\n".format(epsilon, mu, delta))

    // Starting timer...
    timer = System.currentTimeMillis()
    var flocks: Dataset[Flock] = runSpatialJoin(simba, timestamps, pointset, conf)
    val nFlocks = flocks.count()
    if(printFlocks){ // Reporting final set of flocks...
      flocks.orderBy("start", "ids").show(100, truncate = false)
      logger.warn("\n\nFinal flocks: %d\n".format(nFlocks))
    }
    val SJtime = (System.currentTimeMillis() - timer)/1000.0
    logger.warn("Running SpatialJoin variant... [%.3fs] [%d timestamps]".format(SJtime, nFlocks))

    logger.warn("\n\nSpatialJoin,%.1f,%d,%d,%d,%.3f\n\n".format(epsilon, mu, delta, nFlocks, SJtime))

    // Closing all...
    logger.warn("Closing app...")
    simba.close()
  }

  /* SpatialJoin variant */
  def runSpatialJoin(simba: SimbaSession, timestamps: List[Int], pointset: Dataset[ST_Point], conf: Conf): Dataset[Flock] ={
    // Initialize partial result set...
    import simba.implicits._
    import simba.simbaImplicits._

    var FinalFlocks: Dataset[Flock] = simba.sparkContext.emptyRDD[Flock].toDS()
    var nFinalFlocks: Long = 0
    var F_prime: Dataset[Flock] = simba.sparkContext.emptyRDD[Flock].toDS()
    var nF_prime: Long = 0
    var U: Dataset[Flock] = simba.sparkContext.emptyRDD[Flock].toDS()
    var nU: Long = 0
    var timer: Long = 0
    val epsilon: Double = conf.epsilon()
    val mu: Int = conf.mu()
    val delta: Int = conf.delta()
    val partitions: Int = conf.partitions()
    val printIntermediate: Boolean = conf.debug()
    val speed: Double = conf.speed()
    val distanceBetweenTimestamps = speed * delta

    // For each new time instance t_i...
    for(timestamp <- timestamps){
      // Reported location for trajectories in time t_i...
      timer = System.currentTimeMillis()
      val currentPoints = pointset
        .filter(datapoint => datapoint.t == timestamp)
        .map{ datapoint =>
          "%d\t%f\t%f".format(datapoint.id, datapoint.x, datapoint.y)
        }
        .rdd
        .cache()
      val nCurrentPoints = currentPoints.count()
      logger.warn("Reported location for trajectories in time %d... [%.3fs] [%d points]".format(timestamp, (System.currentTimeMillis() - timer)/1000.0, nCurrentPoints))

      // Set of disks for t_i...
      timer = System.currentTimeMillis()
      val C: Dataset[Flock] = MaximalFinderExpansion
        .run(currentPoints, simba, conf)
        .map{ m =>
          val disk = m.split(";")
          val x = disk(1).toDouble
          val y = disk(2).toDouble
          val ids = disk(3)
          Flock(timestamp, timestamp, ids, x, y)
        }.toDS().cache()
      val nC = C.count()
      logger.warn("Set of disks for t_i... [%.3fs] [%d disks]".format((System.currentTimeMillis() - timer)/1000.0, nC))

      var nFlocks: Long = 0
      var nJoin: Long = 0
      /*****************************************************************************/
      if(nF_prime != 0) {
        // Distance Join phase with previous potential flocks...
        timer = System.currentTimeMillis()
        logger.warn("Indexing current maximal disks for timestamps %d...".format(timestamp))
        val cDS = C.index(RTreeType, "cRT", Array("lon", "lat"))
        logger.warn("Indexing previous potential flocks for timestamps %d...".format(timestamp))
        val fDS = F_prime.index(RTreeType, "f_primeRT", Array("lon", "lat"))
        logger.warn("Joining C and F_prime using a distance of %.2fm...".format(distanceBetweenTimestamps))
        val join = fDS.distanceJoin(cDS, Array("lon", "lat"), Array("lon", "lat"), distanceBetweenTimestamps)
        if (printIntermediate) {
          join.show()
        }
        nJoin = join.count()
        logger.warn("Distance Join phase with previous potential flocks... [%.3fs] [%d results]".format((System.currentTimeMillis() - timer) / 1000.0, nJoin))

        // At least mu...
        timer = System.currentTimeMillis()
        val the_mu = conf.mu()
        val U_prime = join.map { tuple =>
            val ids1 = tuple.getString(7).split(" ").map(_.toLong)
            val ids2 = tuple.getString(2).split(" ").map(_.toLong)
            val u = ids1.intersect(ids2)
            val length = u.length
            val s = tuple.getInt(0) // set the initial time...
            val e = timestamp // set the final time...
            val ids = u.sorted.mkString(" ") // set flocks ids...
            val x = tuple.getDouble(8)
            val y = tuple.getDouble(9)
            (Flock(s, e, ids, x, y), length)
          }
          .filter(flock => flock._2 >= the_mu)
          .map(_._1)
        U = pruneFlocks(U_prime, partitions, simba).toDS().cache()
        nU = U.count()
        logger.warn("At least mu... [%.3fs] [%d candidate flocks]".format((System.currentTimeMillis() - timer) / 1000.0, nU))
      } else {
        U = C
        nU = nC
      }
      // Found flocks...
      timer = System.currentTimeMillis()
      val flocks = U.filter(flock => flock.end - flock.start + 1 == delta).cache()
      nFlocks = flocks.count()
      logger.warn("Found flocks... [%.3fs] [%d flocks]".format((System.currentTimeMillis() - timer)/1000.0, nFlocks))

      if(printIntermediate){
        logger.warn("\n\nFound %d flocks on timestamp %d:\n%s\n\n".format(nFlocks, timestamp, flocks.collect().mkString("\n")))
      }

      // Report flock patterns...
      timer = System.currentTimeMillis()
      FinalFlocks = FinalFlocks.union(flocks).cache()
      nFinalFlocks = FinalFlocks.count()
      logger.warn("Report flock patterns... [%.3fs]".format((System.currentTimeMillis() - timer)/1000.0))

      // Update u.t_start. Shift the time...
      timer = System.currentTimeMillis()
      val F = U.filter(flock => flock.end - flock.start + 1 != delta)
        .union(flocks.map(u => Flock(u.start + 1, u.end, u.ids, u.lon, u.lat)))
        .cache()
      val nF = F.count()
      logger.warn("Update u.t_start. Shift the time... [%.3fs] [%d flocks updated]".format((System.currentTimeMillis() - timer)/1000.0, nF))

      // Merge potential flocks U and disks C and adding to F...
      timer = System.currentTimeMillis()
      F_prime = F.union(C)
        .rdd
        .map(f => (f.ids, f))
        .reduceByKey( (a,b) => if(a.start < b.start) a else b )
        .map(_._2)
        .toDS()
        .cache()
      nF_prime = F_prime.count()
      logger.warn("Merge potential flocks U and disks C and adding to F... [%.3fs] [%d flocks added]".format((System.currentTimeMillis() - timer)/1000.0, nF_prime))
      /*****************************************************************************/

      // Reporting summary...
      logger.warn("\n\nPFLOCK_SJ\t%d\t%.1f\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n"
        .format(timestamp, epsilon, mu, delta, nFlocks, nFinalFlocks, nCurrentPoints, nC, nJoin, nU, nF_prime))
    }

    FinalFlocks
  }

  def pruneFlocks(U: Dataset[Flock], partitions: Int, simba: SimbaSession): RDD[Flock] = {
    import simba.implicits._
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

    U_prime.rdd
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
    FlockFinderSpatialJoin.run(conf)
  }
}

