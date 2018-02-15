import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.types.StructType
import org.rogach.scallop.{ScallopConf, ScallopOption}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

object FlockFinder {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class ST_Point(id: Int, x: Double, y: Double, t: Int)
  case class Flock(start: Int, end: Int, ids: List[Long], lon: Double = 0.0, lat: Double = 0.0)

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val epsilon:    ScallopOption[Double] = opt[Double] (default = Some(1.0))
    val mu:         ScallopOption[Int]    = opt[Int]    (default = Some(3))
    val entries:    ScallopOption[Int]    = opt[Int]    (default = Some(25))
    val partitions: ScallopOption[Int]    = opt[Int]    (default = Some(32))
    val candidates: ScallopOption[Int]    = opt[Int]    (default = Some(256))
    val cores:      ScallopOption[Int]    = opt[Int]    (default = Some(32))
    val master:     ScallopOption[String] = opt[String] (default = Some("spark://169.235.27.134:7077")) /* spark://169.235.27.134:7077 */
    val home:       ScallopOption[String] = opt[String] (default = Some("RESEARCH_HOME"))
    val path:       ScallopOption[String] = opt[String] (default = Some("Datasets/Buses/"))
    val valpath:    ScallopOption[String] = opt[String] (default = Some("Validation/"))
    val dataset:    ScallopOption[String] = opt[String] (default = Some("buses0-1"))
    val extension:  ScallopOption[String] = opt[String] (default = Some("tsv"))
    val separator:  ScallopOption[String] = opt[String] (default = Some("\t"))
    val method:     ScallopOption[String] = opt[String] (default = Some("fpmax"))
    val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(true))
    // FlockFinder parameters
    val delta:	    ScallopOption[Int]    = opt[Int]    (default = Some(2))
    val tstart:     ScallopOption[Int]    = opt[Int]    (default = Some(0))
    val tend:       ScallopOption[Int]    = opt[Int]    (default = Some(5))
    val cartesian:  ScallopOption[Int]    = opt[Int]    (default = Some(2))
    val logs:	      ScallopOption[String] = opt[String] (default = Some("INFO"))
    val output:	    ScallopOption[String] = opt[String] (default = Some("/tmp/"))    
    val printIntermediate: ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
    val printFlocks: ScallopOption[Boolean] = opt[Boolean] (default = Some(true))

    verify()
  }
  
  def run(conf: Conf): Unit = {
    // Setting paramaters...
    var timer = System.currentTimeMillis()
    val epsilon: Double = conf.epsilon()
    val mu: Int = conf.mu()
    val delta: Int = conf.delta()
    val tstart: Int = conf.tstart()
    val tend: Int = conf.tend()
    val cartesian: Int = conf.cartesian()
    val partitions: Int = conf.partitions()
    val cores: Int = conf.cores()
    val printIntermediate: Boolean = conf.printIntermediate()
    val printFlocks: Boolean = conf.printFlocks()
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
    var FinalFlocks: RDD[Flock] = simba.sparkContext.emptyRDD
    var nFinalFlocks: Long = 0

    /***************************************
    *     Starting Flock Evaluation...     *
    ***************************************/
    // Initialize partial result set...
    var F_prime: RDD[Flock] = simba.sparkContext.emptyRDD[Flock]
    var nF_prime: Long = 0
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
      val C: RDD[Flock] = MaximalFinderExpansion.
        run(currentPoints, simba, conf).
        repartition(cartesian).
        map{ m => 
          val c = m.split(";")(2).split(" ").toList.map(_.toLong)
          Flock(timestamp, timestamp, c)
        }.cache()
      val nC = C.count()
      logger.warn("Set of disks for t_i... [%.3fs] [%d disks]".format((System.currentTimeMillis() - timer)/1000.0, nC))
        
      // Holds potential flocks up to t...
      timer = System.currentTimeMillis()
      var F = simba.sparkContext.emptyRDD[Flock]
      logger.warn("Holds potential flocks up to t... [%.3fs]".format((System.currentTimeMillis() - timer)/1000.0))

      // Join phase with previous potential flocks...
      timer = System.currentTimeMillis()
      if(printIntermediate){
        logger.warn("Running cartesian function (%d x %d = %d)for timestamps %d...".format(nC, nF_prime, nC*nF_prime, timestamp))
      }
      val combinations = C.cartesian(F_prime).cache()
      val nCombinations = combinations.count()
      logger.warn("Join phase with previous potential flocks... [%.3fs] [%d combinations]".format((System.currentTimeMillis() - timer)/1000.0, nCombinations))

      if(printIntermediate){
        logger.warn("\nPrinting C (%d flocks)\n %s\n".format(nC, Flocks2String(C)))
        logger.warn("\nPrinting F_prime (%d flocks)\n %s\n".format(nF_prime, Flocks2String(F_prime)))
      }

      // At least mu...
      var U = combinations.map{ tuple =>
          val u = tuple._1.ids.intersect(tuple._2.ids).sorted
          val s = tuple._2.start  // set the initial time...
          val e = timestamp       // set the final time...
          Flock(s, e, u)
        }
        .filter(flock => flock.ids.lengthCompare(mu) >= 0)
        .cache()
      if(printIntermediate){
        logger.warn("\nPrinting U before prune (%d flocks)\n %s\n".format(U.count(), Flocks2String(U)))
      }
      U = pruneFlocks(U, partitions)
      var nU = U.count()
      logger.warn("At least mu... [%.3fs] [%d candidate flocks]".format((System.currentTimeMillis() - timer)/1000.0, nU))

      if(printIntermediate){
        logger.warn("\nPrinting U (%d flocks)\n %s\n".format(nU, Flocks2String(U)))
      }

      // Found flocks...
      timer = System.currentTimeMillis()
      val flocks = U.filter(flock => flock.end - flock.start + 1 == delta).cache()
      val nFlocks = flocks.count()
      logger.warn("Found flocks... [%.3fs] [%d flocks]".format((System.currentTimeMillis() - timer)/1000.0, nFlocks))

      if(printIntermediate){
        logger.warn("\n\nFound %d flocks on timestamp %d:\n%s\n\n".format(nFlocks, timestamp, flocks.collect().mkString("\n")))
      }

      // Saving partial flocks...
      timer = System.currentTimeMillis()
      FinalFlocks = FinalFlocks.union(flocks).cache()
      nFinalFlocks = FinalFlocks.count()
      logger.warn("Saving partial flocks...... [%.3fs]".format((System.currentTimeMillis() - timer)/1000.0))

      // Update u.t_start. Shift the time...
      timer = System.currentTimeMillis()
      U = U.filter(flock => flock.end - flock.start + 1 != delta)
        .union(flocks.map(u => Flock(u.start + 1, u.end, u.ids)))
        .cache()
      nU = U.count()
      logger.warn("Update u.t_start. Shift the time... [%.3fs] [%d flocks updated]".format((System.currentTimeMillis() - timer)/1000.0, nFlocks))

      // Add potential flocks U to F...
      timer = System.currentTimeMillis()
      F = F.union(U).cache()
      logger.warn("Add potential flocks U to F... [%.3fs] [%d flocks added]".format((System.currentTimeMillis() - timer)/1000.0, nU))

      // Add disks C to F...
      timer = System.currentTimeMillis()
      F = F.union(C).cache()
      logger.warn("Add disks C to F... [%.3fs] [%d flocks added]".format((System.currentTimeMillis() - timer)/1000.0, nC))

      // Moving to next timestamp...
      timer = System.currentTimeMillis()
      F_prime = F
      F_prime.cache()
      nF_prime = F_prime.count()
      logger.warn("Moving to next timestamp... [%.3fs] [%d candidate flocks moved]".format((System.currentTimeMillis() - timer)/1000.0, nF_prime))

      logger.warn("\n\nPFLOCK\t%d\t%.1f\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n"
        .format(timestamp, epsilon, mu, delta, nFlocks, nFinalFlocks, nCurrentPoints, nC, nCombinations, nU, nF_prime))
    }

    if(printFlocks){ // Reporting final set of flocks...
      val finalFlocks = FinalFlocks.collect()
      val nFinalFlocks = finalFlocks.length
      val flocksReport = finalFlocks
        .map{ f =>
          "%d, %d, %s\n".format(f.start, f.end, f.ids.mkString(" "))
        }
        .mkString("")
      logger.warn("\n\nFinal flocks: %d\n%s\n".format(nFinalFlocks, flocksReport))
    }

    // Closing all...
    logger.warn("Closing app...")
    simba.close()
  }
  
  def saveStringArray(array: Array[String], tag: String, conf: Conf): Unit = {
    val path = s"/tmp/"
    val filename = s"${conf.dataset()}_E${conf.epsilon()}_M${conf.mu()}_D${conf.delta()}"
    new java.io.PrintWriter("%s%s_%s.txt".format(path, filename, tag)) {
      write(array.mkString("\n"))
      close()
    }
  }
  
  def saveFlocks(array: Array[String], conf: Conf): Unit = {
    val epsilon = conf.epsilon().toInt
    val mu = conf.mu()
    val delta = conf.delta()
    
    val filename = "PFLOCK_E%d_M%d_D%d".format(epsilon, mu, delta)
    new java.io.PrintWriter("%s%s.txt".format(conf.output(), filename)) {
      write(array.mkString(""))
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
            val ids1 = flocks(i)._1.ids
            val ids2 = flocks(j)._1.ids
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
              val ids1 = flocks(i)._1.ids
              val ids2 = flocks(j)._1.ids
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
      "\n%d,%d,%s".format(f.start, f.end, f.ids.mkString(" "))
    }.collect.mkString("")
    
    "# of flocks: %d\n%s".format(n,info)
  }

  def main(args: Array[String]): Unit = {
    logger.info("Starting app...")
    val conf = new Conf(args)
    FlockFinder.run(conf)
  }
}
