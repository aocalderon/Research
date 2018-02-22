import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

object FlockFinderSpatialJoin {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class ST_Point(id: Int, x: Double, y: Double, t: Int)
  case class Flock(start: Int, end: Int, ids: String, lon: Double = 0.0, lat: Double = 0.0)

  def run(conf: Conf): Unit = {
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
      .filter(datapoint => datapoint.t >= tstart && datapoint.t <= tend)
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
    logger.warn("\n\n*** Epsilon=%.1f, Mu=%d and Delta=%d ***\n".format(epsilon, mu, delta))

    // Storing final set of flocks...
    simba.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) // To improve cartesian product performance...
    var FinalFlocks: RDD[Flock] = simba.sparkContext.emptyRDD
    var nFinalFlocks: Long = 0

    /***************************************
    *     Starting Flock Evaluation...     *
    ***************************************/
    // Initialize partial result set...
    var F_prime: RDD[Flock] = simba.sparkContext.emptyRDD[Flock]
    var nF_prime: Long = 0
    var U: RDD[Flock] = simba.sparkContext.emptyRDD[Flock]
    var nU: Long = 0
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
      var C: RDD[Flock] = MaximalFinderExpansion
        .run(currentPoints, simba, conf)
        .map{ m =>
          val ids = m.split(";")(2)
          Flock(timestamp, timestamp, ids)
        }.cache()

      val nC = C.count()
      logger.warn("Set of disks for t_i... [%.3fs] [%d disks]".format((System.currentTimeMillis() - timer)/1000.0, nC))
        
      // Getting flock coordinates...
      timer = System.currentTimeMillis()
      C = getCoordinates(C, currentPoints, timestamp, simba)
      logger.warn("Getting flock coordinates... [%.3fs] [%d disks located]".format((System.currentTimeMillis() - timer)/1000.0, nC))
      var nFlocks: Long = 0
      var nJoin: Long = 0

      if (printIntermediate) {
        logger.warn("\nPrinting C (%d flocks)\n %s\n".format(nC, Flocks2String(C)))
        logger.warn("\nPrinting F_prime (%d flocks)\n %s\n".format(nF_prime, Flocks2String(F_prime)))
        val filename1 = "/tmp/C_T%d".format(timestamp)
        saveFlocks(C, filename1)
        val filename2 = "/tmp/F_prime_T%d".format(timestamp)
        saveFlocks(F_prime, filename2)
      }

      /*****************************************************************************/
      if(nF_prime != 0) {
        // Distance Join phase with previous potential flocks...
        timer = System.currentTimeMillis()
        logger.warn("Indexing current maximal disks and previous potential flocks for timestamps %d...".format(timestamp))
        import simba.simbaImplicits._
        val cDS = C.toDS().index(RTreeType, "cRT", Array("lon", "lat"))
        val fDS = F_prime.toDS().index(RTreeType, "f_primeRT", Array("lon", "lat"))
        logger.warn("Joining sets using a distance of %.2fm...".format(distanceBetweenTimestamps))
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
        .rdd
        .cache()

        if (printIntermediate) {
          logger.warn("\nU_prime before prune: %d flocks\n".format(U_prime.count()))
          val filename = "/tmp/U_prime_T%d".format(timestamp)
          saveFlocks(U_prime, filename)
        }
        U = pruneFlocks(U_prime, partitions).cache()
        nU = U.count()
        if (printIntermediate) {
          logger.warn("\nU after prune: %d flocks\n".format(U.count()))
          val filename = "/tmp/U_T%d".format(timestamp)
          saveFlocks(U, filename)
        }
        logger.warn("At least mu... [%.3fs] [%d candidate flocks]".format((System.currentTimeMillis() - timer) / 1000.0, nU))

        if (printIntermediate) {
          logger.warn("\nPrinting U (%d flocks)\n %s\n".format(nU, Flocks2String(U)))
        }
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
        .map(f => (f.ids, f))
        .reduceByKey( (a,b) =>
          if(a.start < b.start){
            a
          } else {
            b
          }
        )
        .map(_._2)
        .cache()
      nF_prime = F_prime.count()
      logger.warn("Merge potential flocks U and disks C and adding to F... [%.3fs] [%d flocks added]".format((System.currentTimeMillis() - timer)/1000.0, nF_prime))
      /*****************************************************************************/

      // Reporting summary...
      logger.warn("\n\nPFLOCK\t%d\t%.1f\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n"
        .format(timestamp, epsilon, mu, delta, nFlocks, nFinalFlocks, nCurrentPoints, nC, nJoin, nU, nF_prime))
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
    simba.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)
    simba.close()
  }
  
  /* Extract coordinates from a set of point IDs */
  def getCoordinates(C: RDD[Flock], currentPoints: RDD[String], timestamp: Int, simba: SimbaSession): RDD[Flock] = {
    import simba.implicits._

    val C_prime = C.map(c => (c.ids, c.ids.split(" ").map(_.toInt)))
      .toDF("ids", "idsList")
      .withColumn("id", explode($"idsList"))
      .select("ids", "id")
    val P_prime = currentPoints.map{ p => 
      val r = p.split("\t")
      (r(0).toLong, r(1).toDouble, r(2).toDouble)
    }
    .toDF("id","x","y")
    .cache()
    
    C_prime.join(P_prime, C_prime.col("id") === P_prime.col("id"))
      .select("ids", "x", "y")
      .map(p => (p.getString(0), (p.getDouble(1), p.getDouble(2))))
      .rdd
      .reduceByKey( (a, b) =>
        ( a._1 + b._1, a._2 + b._2 )
      )
      .map{ f => 
        val n = f._1.split(" ").length
        Flock(timestamp, timestamp, f._1, f._2._1 / n, f._2._2 / n)
      }
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
          "%d, %d, %s\n".format(f.start, f.end, f.ids)
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
