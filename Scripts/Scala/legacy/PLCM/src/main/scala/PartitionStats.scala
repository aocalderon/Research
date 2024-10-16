import com.vividsolutions.jts.geom.{Point, Geometry, GeometryFactory, Coordinate, Envelope, Polygon}
import org.slf4j.{LoggerFactory, Logger}
import org.rogach.scallop._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialRDD.{CircleRDD, PointRDD}
import com.vividsolutions.jts.index.strtree.STRtree
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._
import SPMF.{AlgoLCM2, Transactions}
import com.vividsolutions.jts.index.strtree.STRtree
import org.apache.spark.Partitioner
import java.io._

object PartitionsStats{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory()
  private var num: Long = 0
  private var max: Int = 0
  private var avg: Double = 0.0
  private var statsTime1 = 0.0
  private var statsTime2 = 0.0

  def main(args: Array[String]) = {
    val params = new PStatsConf(args)
    val input = params.input()
    val epsilon = params.epsilon()
    val master = params.master()
    val partitions = params.partitions()
    val offset = params.offset()
    val spatial = params.spatial()
    val cores = params.cores()
    val executors = params.executors()
    val stats = !params.stats()
    val debug = params.debug()

    // Starting session...
    var timer = clocktime
    val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .master(master).appName("PartitionsStats")
      .config("spark.cores.max", cores * executors)
      .config("spark.executor.cores", cores)
      .getOrCreate()
    import spark.implicits._
    val appID = spark.sparkContext.applicationId
    val appIDFile = new PrintWriter("/tmp/SparkAppID")
    appIDFile.write(appID)
    appIDFile.close()
    logger.info(s"Session $appID started [${(clocktime - timer) / 1000.0}]")

    // Reading disks...
    timer = clocktime
    val disks = new PointRDD(spark.sparkContext, input, offset, FileDataSplitter.TSV, true)
    val nDisks = disks.rawSpatialRDD.count()
    log("Disks read", timer, nDisks)

    // Partitioning disks...
    timer = clocktime
    val partitioner = spatial  match {
      case "QUADTREE" => GridType.QUADTREE
      case "RTREE"    => GridType.RTREE
      case "EQUAL"    => GridType.EQUALGRID
      case "KDBTREE"  => GridType.KDBTREE
      case "HILBERT"  => GridType.HILBERT
      case "VORONOI"  => GridType.VORONOI
    }
    val startTime = clocktime
    disks.analyze()
    disks.spatialPartitioning(partitioner, partitions)
    log("Disks partitioned", timer, nDisks)

    if(debug){
      val dataset = input.split("/").last.split("\\.").head
      val points = disks.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ (index, partition) =>
        partition.map{ disk =>
          val pids = disk.getUserData.toString()
          (disk.getX, disk.getY, pids, index)
        }
      }.persist(StorageLevel.MEMORY_ONLY)
      val nPoints = points.count()
      val pw1 = new PrintWriter(s"/tmp/${dataset}_${partitioner.toString()}_points.wkt")
      var wkt = points.map{ p => s"POINT(${p._1} ${p._2});${p._3};${p._4}\n"}.collect().mkString("")
      pw1.write(wkt)
      pw1.close()
      val pw2 = new PrintWriter(s"/tmp/${dataset}_${partitioner.toString()}_partitions.wkt")
      wkt = disks.getPartitioner.getGrids().asScala.map{ e =>
        val minx = e.getMinX
        val miny = e.getMinY
        val maxx = e.getMaxX
        val maxy = e.getMaxY
        s"POLYGON(($minx $miny, $minx $maxy, $maxx $maxy, $maxx $miny, $minx $miny))\n"
      }.mkString("")
      pw2.write(wkt)
      pw2.close()
    }

    if(stats){
      // Computing pre-expansion statistics...
      timer = clocktime
      val stats1 = disks.spatialPartitionedRDD.rdd.mapPartitions{ p =>
        List(p.length).toIterator
      }
      num = stats1.count()
      max = stats1.max()
      avg = (stats1.sum() / stats1.count()).toInt
      statsTime1 = (clocktime - timer) / 1000.0
      log(s"Pre-expansion statistics  ($num, $max, $avg)", timer, max)
    }

    // Building expansions...
    timer = clocktime
    val rtree = new STRtree()
    val expansions = disks.getPartitioner.getGrids.asScala.map{ e =>
      new Envelope(e.getMinX - epsilon, e.getMaxX + epsilon, e.getMinY - epsilon, e.getMaxY + epsilon)
    }.zipWithIndex
    expansions.foreach{e => rtree.insert(e._1, e._2)}
    val expansions_map = expansions.map(e => e._2 -> e._1).toMap

    val expansionsRDD = disks.spatialPartitionedRDD.rdd.flatMap{ disk =>
      rtree.query(disk.getEnvelopeInternal).asScala.map{expansion_id =>
        (expansion_id, disk)
      }.toList
    }.partitionBy(new ExpansionPartitioner(expansions.size)).persist(StorageLevel.MEMORY_ONLY)
    //val nExpansionsRDD = 0
    val nExpansionsRDD = expansionsRDD.count()
    log("Expansions built", timer, nExpansionsRDD)

    if(stats){
      // Computing expansion statistics...
      timer = clocktime
      val stats2 = expansionsRDD.mapPartitions{ p =>
        List(p.length).toIterator
      }
      num = stats2.count()
      max = stats2.max()
      avg = (stats2.sum() / stats2.count()).toInt
      statsTime2 = (clocktime - timer) / 1000.0
      log(s"Post-expansion statistics ($num, $max, $avg)", timer, max)
    }

    // Prunning disks...
    timer = clocktime
    val points = expansionsRDD.map(_._2).mapPartitionsWithIndex{ (index, partition) =>
      val transactions = partition.map{ d =>
        d.getUserData.toString().split("\t")(0).split(" ").map(new Integer(_)).toList.sorted.asJava
      }.toList.asJava
      val LCM = new AlgoLCM2()
      val data = new Transactions(transactions)
      LCM.run(data).asScala.map{ m =>
        (m.asScala.mkString(" "), index)
      }.toIterator
    }.persist(StorageLevel.MEMORY_ONLY)
    //val nPoints = 0
    val nPoints = points.count()
    log("Disks prunned", timer, nPoints)

    // Prunning results...
    timer = clocktime
    val f0 = disks.rawSpatialRDD.rdd
      .map{ d =>
        val arr = d.getUserData.toString().split("\t")
        val pids = arr(0)
        val s = arr(1).toInt
        val e = arr(2).toInt
        (pids, s, e, d.getX, d.getY)
      }.toDF("pids","s","e","x","y")
    val f1 = points.toDF("pids", "eid")
    val prunned = f0.join(f1, "pids")
      .select($"eid", $"pids", $"x", $"y", $"s", $"e")
      .map{ m =>
        val expansion = expansions_map(m.getInt(0))
        val pids = m.getString(1)
        val x = m.getDouble(2)
        val y = m.getDouble(3)
        val s = m.getInt(4)
        val e = m.getInt(5)
        val point = geofactory.createPoint(new Coordinate(x, y))
        val notInExpansion = isNotInExpansionArea(point, expansion, epsilon)
        val f = s"${pids};${s};${e};${x};${y}"
        (f, notInExpansion, expansion.toString())
      }.rdd.filter(_._2).map(_._1)
      .distinct()
      .persist(StorageLevel.MEMORY_ONLY)
    val nPrunned = prunned.count()
    log("Results prunned", timer, nPrunned)
    val endTime = clocktime
    val time = "%.2f".format(((endTime - startTime) / 1000.0) - statsTime1 - statsTime2)
    val ttime = "%.2f".format(((endTime - startTime) / 1000.0))
    logger.info(s"PLCM;$cores;$executors;$partitions;$num;$max;$avg;$nExpansionsRDD;$time;$ttime;$nPoints;$nPrunned;$spatial;$appID")
    val url = s"http://localhost:4040/api/v1/applications/${appID}/executors"
    val r = requests.get(url)
    if(s"${r.statusCode}" == "200"){
      import scala.util.parsing.json._
      val j = JSON.parseFull(r.text).get.asInstanceOf[List[Map[String, Any]]]
      j.filter(_.get("id").get != "driver").foreach{ m =>
        val id     = m.get("id").get
        val ttasks = "%.0f".format(m.get("totalTasks").get)
        val tcores = "%.0f".format(m.get("totalCores").get)
        val ttime  = "%.2fs".format(m.get("totalDuration").get.asInstanceOf[Double] / 1000.0)
        val tinput = "%.2fMB".format(m.get("totalInputBytes").get.asInstanceOf[Double] / (1024.0 * 1024))
        logger.info(s"EXECUTORS;$appID;$id;$tcores;$ttasks;$ttime;$tinput")
      }
    }

    // Closing session...
    timer = clocktime
    spark.close()
    logger.info(s"Session $appID [${(clocktime - timer) / 1000.0}]")
  }

  class ExpansionPartitioner(partitions: Int) extends Partitioner{
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      key.asInstanceOf[Int]
    }
  }  

  def isNotInExpansionArea(p: Point, e: Envelope, epsilon: Double): Boolean = {
    val error = 0.00000000001
    val x = p.getX
    val y = p.getY
    val min_x = e.getMinX - error
    val min_y = e.getMinY - error
    val max_x = e.getMaxX
    val max_y = e.getMaxY

    x <= (max_x - epsilon) &&
      x >= (min_x + epsilon) &&
      y <= (max_y - epsilon) &&
      y >= (min_y + epsilon)
  }

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long = 0): Unit = {
    if(n == 0)
      logger.info("%-50s|%6.2f".format(msg,(clocktime - timer)/1000.0))
    else
      logger.info("%-50s|%6.2f|%6d".format(msg,(clocktime - timer)/1000.0,n))
  }
}

class PStatsConf(args: Seq[String]) extends ScallopConf(args) {
  val input:      ScallopOption[String]  = opt[String]  (required = true)
  val master:     ScallopOption[String]  = opt[String]  (default = Some("spark://169.235.27.134:7077"))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val executors:  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val spatial:    ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val epsilon:    ScallopOption[Int]     = opt[Int]     (default = Some(10))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(24))
  val offset:     ScallopOption[Int]     = opt[Int]     (default = Some(0))
  val stats:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
