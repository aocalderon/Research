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

object PLCM{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory()

  def main(args: Array[String]) = {
    val params = new PCLMConf(args)
    val input = params.input()
    val epsilon = params.epsilon()
    val master = params.master()
    val partitions = params.partitions()
    val offset = params.offset()
    val spatial = params.spatial()
    val debug = params.debug()

    // Starting session...
    var timer = clocktime
    val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .master(master).appName("PLCM").getOrCreate()
    import spark.implicits._
    logger.info(s"Session started [${(clocktime - timer) / 1000.0}]")

    // Reading disks...
    timer = clocktime
    val disks = new PointRDD(spark.sparkContext, input, offset, FileDataSplitter.TSV, true, partitions)
    val nDisks = disks.rawSpatialRDD.count()
    log("Disks read", timer, nDisks)

    /* Partitioner Benchmark...

    val partitioners = List(GridType.QUADTREE, GridType.KDBTREE, GridType.EQUALGRID, GridType.RTREE)
    val partition_set = 5 to 100 by 5
    for(partitioner <- partitioners; partition <- partition_set){
      timer = clocktime
      disks.analyze()
      disks.spatialPartitioning(GridType.QUADTREE, partition)
      logger.info(s";${partitioner.toString()};${partition};${(clocktime-timer)/1000.0}")
    }
     */

    // Partitioning disks...
    timer = clocktime
    val partitioner = spatial  match {
      case "QUADTREE" => GridType.QUADTREE
      case "RTREE" => GridType.RTREE
      case "EQUAL" => GridType.EQUALGRID
      case "KDBTREE" => GridType.KDBTREE
      case "HILBERT" => GridType.HILBERT
      case "VORONOI" => GridType.VORONOI
    }
    disks.analyze()
    disks.spatialPartitioning(partitioner, partitions)
    log("Disks partitioned", timer, nDisks)

    // Computing partition statistics...
    val stats = disks.spatialPartitionedRDD.rdd.mapPartitions{ p =>
      List(p.length).toIterator
    }
    logger.info(s"Number of partitions: ${stats.count()}")
    logger.info(s"Max: ${stats.max()}, Min: ${stats.min()}, Avg: ${stats.sum()/stats.count()}")

    if(debug){
      val points = disks.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ (index, partition) =>
        partition.map{ disk =>
          val pids = disk.getUserData.toString()
          (disk.getX, disk.getY, pids, index)
        }
      }.persist(StorageLevel.MEMORY_ONLY)
      val nPoints = points.count()
      val pw1 = new PrintWriter(s"/tmp/${partitioner.toString()}_points.wkt")
      var wkt = points.map{ p => s"POINT(${p._1} ${p._2});${p._3};${p._4}\n"}.collect().mkString("")
      pw1.write(wkt)
      pw1.close()
      val pw2 = new PrintWriter(s"/tmp/${partitioner.toString()}_partitions.wkt")
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
    val nExpansionsRDD = expansionsRDD.count()
    log("Expansions built", timer, nExpansionsRDD)

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
    val nPoints = points.count()
    log("Disks prunned", timer, nPoints)

    points.toDF().show(10, true)

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
    val prunned0 = f0.join(f1, "pids")
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
      }.rdd

    val prunned = prunned0.filter(_._2).map(_._1)
      .distinct()
      .persist(StorageLevel.MEMORY_ONLY)
    val nPrunned = prunned.count()
    log("Results prunned", timer, nPrunned)

    // Closing session...
    timer = clocktime
    spark.close()
    logger.info(s"Session closed [${(clocktime - timer) / 1000.0}]")
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

class PCLMConf(args: Seq[String]) extends ScallopConf(args) {
  val input:      ScallopOption[String]  = opt[String]  (required = true)
  val master:     ScallopOption[String]  = opt[String]  (default = Some("spark://169.235.27.134:7077"))
  val spatial:    ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val epsilon:    ScallopOption[Int]     = opt[Int]     (default = Some(10))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(24))
  val offset:     ScallopOption[Int]     = opt[Int]     (default = Some(0))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
