import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.scalameter._
import org.slf4j.{Logger, LoggerFactory}

object BasicSpatialOps {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  var epsilon = 0.0
  val precision = 0.01
  val master = "local[*]" //"spark://169.235.27.134:7077"
  var cores = 4
  var file1 = ""
  var file2 = ""
  var timer = new Quantity[Double](0.0, "ms")
  var clock = 0.0

  case class Disk(t: Int, ids: String, lon: Double, lat: Double)

  def main(args: Array[String]): Unit = {
    clock = System.nanoTime()
    file1 = "/tmp/D_i.txt"
    file2 = "/tmp/D_prime.txt"

    val simba = SimbaSession.builder().master(master).
      appName("Benchmark").
      //config("simba.join.partitions", "32").
      config("simba.index.partitions", "16").
      getOrCreate()
    logger.info("Starting session,%.2f,%d".format((System.nanoTime() - clock)/1e9d, 0))
    runJoinQuery(simba)
    simba.stop()
  }

  private def runJoinQuery(simba: SimbaSession): Unit = {
    clock = System.nanoTime()
    import simba.implicits._
    import simba.simbaImplicits._
    logger.info("Setting variables,%.2f,%d".format((System.nanoTime() - clock)/1e9d, 0))
    clock = System.nanoTime()
    var left = simba.sparkContext.
      textFile(file1).
      map { line =>
        val lineArray = line.split(",")
        val t = lineArray(0).toInt
        val ids = lineArray(1)
        val x = lineArray(2).toDouble
        val y = lineArray(3).toDouble
        Disk(t, ids, x, y)
      }.toDS() //NO CACHE!!!
    var nLeft = left.count()

    /*
    var right = simba.sparkContext.
      textFile(file2).
      map { line =>
        val lineArray = line.split(",")
        val t = lineArray(0).toInt
        val ids = lineArray(1)
        val x = lineArray(2).toDouble
        val y = lineArray(3).toDouble
        Disk(t, ids, x, y)
      }.toDS()
      */
    var right = (0 until 1).map(d => Disk(-1,"",0.0,0.0)).toDS
    var nRight = right.count()
    logger.info("Reading datasets,%.2f,%d".format((System.nanoTime() - clock)/1e9d, 0))
    logger.info("D_i partitions: " + left.rdd.getNumPartitions)
    logger.info("D_prime partitions: " + right.rdd.getNumPartitions)
    timer = measure {
      left = left.index(RTreeType, "leftRT", Array("lon", "lat")).cache()
      nLeft = left.count()
    }
    logInfo("01.Indexing D_i", timer.value, nLeft)
    timer = measure {
      if(nRight > 0){
        right = right.index(RTreeType, "rightRT", Array("lon", "lat")).cache()
        nRight = right.count()
      }
    }
    logInfo("02.Indexing D_prime", timer.value, nRight)
    logger.info("D_i partitions: " + left.rdd.getNumPartitions)
    logger.info("D_prime partitions: " + right.rdd.getNumPartitions)
    clock = System.nanoTime()
    val join = left
      .distanceJoin(right.toDF("t2", "ids2", "lon2", "lat2"), Array("lon", "lat"), Array("lon2", "lat2"), 10.0)
      .cache()
    val nJoin = join.count()
    join.show(truncate = false)
    logInfo("03.Joining datasets", (System.nanoTime() - clock) / 1e6d, nJoin)
  }
  
  private def logInfo(msg: String, millis: Double, n: Long): Unit = {
    logger.info("%s... [%.3f] [%d]".format(msg, millis / 1000.0, n))
  }
}
