import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.simba.{Dataset, SimbaSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object MaximalDistanceFinder {
  val logger: Logger = LoggerFactory.getLogger("myLogger")
  var conf: Conf = new Conf(Array.empty[String])
  var timer: Long = 0L
  
  case class ST_Point(id: Long, x: Double, y: Double, t: Int = -1)
  case class Flock(start: Int, end: Int, ids: String, x: Double = 0.0, y: Double = 0.0)

  def main(args: Array[String]): Unit = {
    logger.info("Starting app...")
    timer = System.currentTimeMillis()
    conf = new Conf(args)
    val master = conf.master()
    val partitions = conf.partitions()
    val cores = conf.cores()
    val epsilon = conf.epsilon()
    val mu = conf.mu()

    val simba = SimbaSession.builder()
      .master(master)
      .appName("MaximalDistanceFinder")
      .config("simba.index.partitions", partitions)
      .config("spark.cores.max", cores)
      .getOrCreate()
    simba.sparkContext.setLogLevel(conf.logs())
    
    import simba.implicits._
    import simba.simbaImplicits._
    var flocks: Dataset[Flock] = simba.sparkContext.emptyRDD[Flock].toDS
    var nFlocks: Long = 0
    logging("Starting session", timer)

    // Setting paramaters...
    timer = System.currentTimeMillis()
    val separator: String = conf.separator()
    val path: String = conf.path()
    val dataset: String = conf.dataset()
    val extension: String = conf.extension()
    val home: String = scala.util.Properties.envOrElse(conf.home(), "/home/and/Documents/PhD/Research/")
    val point_schema = ScalaReflection
      .schemaFor[ST_Point]
      .dataType
      .asInstanceOf[StructType]
    logging("Setting paramaters", timer)

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
    logging("Reading data", timer, nPointset, "points")
    
    // Distance self-join
    timer = System.currentTimeMillis()
    val sample = pointset.filter(_.t == 0).sample(false, 0.5, 42).cache()
    sample.orderBy("id").show(truncate =false)
    val p1 = sample.select("id", "x", "y").toDF("id1", "x1", "y1")
    p1.index(RTreeType, "rtP1", Array("x1", "y1"))
    val p2 = sample.select("id", "x", "y").toDF("id2", "x2", "y2")
    p2.index(RTreeType, "rtP2", Array("x2", "y2"))
    val join = p1.distanceJoin(p2, Array("x1", "y1"), Array("x2", "y2"), epsilon).filter(j => j.getLong(0) < j.getLong(3)).cache()
    val nJoin = join.count()
    join.orderBy("id1", "id2").show(truncate = false)
    logging("Self-join", timer, nJoin, "pairs")
    
    // Group By
    timer = System.currentTimeMillis()
    val groups = join.groupBy("id1").agg(collect_list("id2").as("ids"), collect_list("x2").as("xs"), collect_list("y2").as("ys")).select("id1", "ids", "xs", "ys")
      .filter(g => g.getList[Long](1).size >= mu)
      .map{ g => 
        val fid = g.getLong(0)
        val ids = g.getList[Long](1).asScala.toList
        val Xs  = g.getList[Double](2).asScala.toList
        val Ys  = g.getList[Double](3).asScala.toList
        val points = ids.zip(Xs.zip(Ys)).map(p => s"${p._1},${p._2._1},${p._2._2}").mkString(";")
        
        (fid, points)
      }
      .toDF("fid", "points")
    val nGroups = groups.count()
    groups.orderBy("fid").show(truncate = false)
    logging("Self-join", timer, nGroups, "groups")
    
    // Mesuring maximum distance...
    timer = System.currentTimeMillis()
    val distances = groups.map{ g => 
        val fid = g.getLong(0)
        val points = g.getString(1).split(";")
          .map{ p => 
            val m = p.split(",")
            ST_Point(m(0).toLong, m(1).toDouble, m(2).toDouble)
          }
        val r = getMaximalDistance(points)
        (fid, r._1, r._2, r._3)
      }
      .toDF("fid", "id1", "id2", "d")
      .filter("d > 90")
    val nDistances = distances.count()
    distances.orderBy("fid").show(truncate = false)
    logging("Mesuring maximum distance...", timer, nDistances, "distances")
    
  }
  
  def getMaximalDistance(p: Array[ST_Point]): (Long, Long, Double) ={
    val n: Int = p.length
    var id1: Long = 0
    var id2: Long = 0
    var max: Double = 0.0
    for(i <- Range(0, n - 1)){
      for(j <- Range(i + 1, n)) {
        val p1 = p(i)
        val p2 = p(j)
        val temp = dist(p1, p2)
        if(temp > max){
          max = temp
          id1 = p1.id
          id2 = p2.id
        }
      }
    }
    (id1, id2, max)
  }

  import Math.{pow, sqrt}
  def dist(p1: ST_Point, p2: ST_Point): Double ={
    sqrt(pow(p1.x - p2.x, 2) + pow(p1.y - p2.y, 2))
  }
  
  def logging(msg: String, timer: Long, n: Long = 0, tag: String = ""): Unit ={
    logger.info("%-50s | %6.2fs | %6d %s".format(msg, (System.currentTimeMillis() - timer)/1000.0, n, tag))
  }
}
