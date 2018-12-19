import com.vividsolutions.jts.geom.{Point, Geometry, GeometryFactory, Coordinate, Envelope, Polygon}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap

object FF {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val precision: Double = 0.001
  private var tag: String = ""

  /* SpatialJoin variant */
  def runSpatialJoin(spark: SparkSession, pointset: HashMap[Int, PointRDD], params: FFConf): Unit = {
    val sespg = params.sespg()
    val tespg = params.tespg()
    var timer = System.currentTimeMillis()

    // Maximal disks timestamp i...
    for(timestamp <- pointset.keys.toList.sorted){
      val T_i = pointset.get(timestamp).get
      logger.info(s"Starting maximal disks timestamp $timestamp ...")
      val disks = MF.run(spark, T_i, params, s"$timestamp")
      val nDisks = disks.count()
      log(s"Maximal disks timestamp $timestamp", timer, nDisks)
    }


    // Initialize partial result set...

    /*
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
      logging("Reporting locations...", timer, nCurrentPoints, "points")

      // Set of disks for t_i...
      timer = System.currentTimeMillis()
      val C: Dataset[Flock] = MaximalFinderExpansion
        .run(currentPoints, epsilon, mu, simba, conf, timestamp)
        .map{ m =>
          val disk = m.split(";")
          val x = disk(0).toDouble
          val y = disk(1).toDouble
          val ids = disk(2)
          Flock(timestamp, timestamp, ids, x, y)
        }.toDS().cache()
      val nC = C.count()
      logging("1.Set of disks for t_i...", timer, nC, "disks")

      var nFlocks: Long = 0
      var nJoin: Long = 0
      if(nF_prime != 0) {
        // Distance Join phase with previous potential flocks...
        timer = System.currentTimeMillis()
        val cDS = C.index(RTreeType, "cRT", Array("x", "y"))
        val fDS = F_prime.index(RTreeType, "f_primeRT", Array("x", "y"))
        val join = fDS.distanceJoin(cDS, Array("x", "y"), Array("x", "y"), distanceBetweenTimestamps)
        nJoin = join.count()
        logging("2.Distance Join phase...", timer, nJoin, "combinations")

        // At least mu...
        timer = System.currentTimeMillis()
        val the_mu = conf.mu()
        val U_prime = join
          .map { tuple =>
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
          .map(_._1).as[Flock]
        U = pruneFlocks(U_prime, simba).cache()
        nU = U.count()
        logging("3.Getting candidates...", timer, nU, "candidates")
      } else {
        U = C
        nU = nC
      }
      // Found flocks...
      timer = System.currentTimeMillis()
      val flocks = U.filter(flock => flock.end - flock.start + 1 == delta).cache()
      nFlocks = flocks.count()
      logging("4.Found flocks...", timer, nFlocks, "flocks")

      // Report flock patterns...
      FinalFlocks = FinalFlocks.union(flocks).cache()
      nFinalFlocks = FinalFlocks.count()

      // Update u.t_start. Shift the time...
      timer = System.currentTimeMillis()
      val F = U.filter(flock => flock.end - flock.start + 1 != delta)
        .union(flocks.map(u => Flock(u.start + 1, u.end, u.ids, u.x, u.y)))
        .cache()
      val nF = F.count()
      logging("5.Updating times...", timer, nF, "flocks")

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
      logging("6.Filter phase...", timer, nF_prime, "flocks")
      if(debug) printFlocks(F_prime, "", simba)
    }
    // Reporting summary...
    logger.warn("\n\nPFLOCK_SJ\t%.1f\t%d\t%d\t%d\n"
      .format(epsilon, mu, delta, nFinalFlocks))

    FinalFlocks
     */
  }

  def main(args: Array[String]): Unit = {
    val params = new FFConf(args)
    val master = params.master()
    val input  = params.input()
    val offset = params.offset()
    val ppartitions = params.ppartitions()
    tag = params.tag()

    // Starting session...
    var timer = System.currentTimeMillis()
    val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .master(master).appName("PFLock").getOrCreate()
    logger.info(s"Session started [${(System.currentTimeMillis - timer) / 1000.0}]")

    // Reading data...
    timer = System.currentTimeMillis()
    val points = new PointRDD(spark.sparkContext, input, offset, FileDataSplitter.TSV, true, ppartitions)
      points.CRSTransform(params.sespg(), params.tespg())
    val nPoints = points.rawSpatialRDD.count()
    val timestamps = points.rawSpatialRDD.rdd.map(_.getUserData.toString().split("\t").reverse.head.toInt).distinct.collect()
    val pointset: HashMap[Int, PointRDD] = new HashMap()
    for(timestamp <- timestamps){
      pointset += (timestamp -> extractTimestamp(points, timestamp, params.sespg(), params.tespg()))
    }
    logger.info(s"Data read [${(System.currentTimeMillis - timer) / 1000.0}] [$nPoints]")

    // Running maximal finder...
    timer = System.currentTimeMillis()
    runSpatialJoin(spark: SparkSession, pointset: HashMap[Int, PointRDD], params: FFConf)
    logger.info(s"Maximal finder run [${(System.currentTimeMillis() - timer) / 1000.0}]")

    // Closing session...
    timer = System.currentTimeMillis()
    spark.stop()
    logger.info(s"Session closed [${(System.currentTimeMillis - timer) / 1000.0}]")    
  }

  def extractTimestamp(points: PointRDD, timestamp:  Int, sespg: String, tespg: String): PointRDD = {
    new PointRDD(points.rawSpatialRDD.rdd.filter{
      _.getUserData().toString().split("\t").reverse.head.toInt == timestamp
    }.toJavaRDD(), StorageLevel.MEMORY_ONLY, sespg, tespg)
  }

  def log(msg: String, timer: Long, n: Long = 0): Unit ={
    if(n == 0)
      logger.info("%-50s|%6.2f".format(msg,(System.currentTimeMillis()-timer)/1000.0))
    else
      logger.info("%-50s|%6.2f|%6d|%s".format(msg,(System.currentTimeMillis()-timer)/1000.0,n,tag))
  }
}
