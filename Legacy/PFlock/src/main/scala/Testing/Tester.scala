package Testing

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.simba.index._
import org.apache.spark.sql.simba.{Dataset, SimbaSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

object Tester {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val ST_Point_schema = ScalaReflection.schemaFor[ST_Point].dataType.asInstanceOf[StructType]
  val input = "/home/acald013/Research/Datasets/Berlin/berlin0-9.tsv"
  val epsilon = 30
  val mu = 6
  val precision = 0.001
  val master = "spark://169.235.27.134:7077" //"spark://169.235.27.134:7077"
  val cores = 21
  val partitions = 1024

  case class ST_Point(id: Int, x: Double, y: Double, t: Double = -1.0)

  def main(args: Array[String]): Unit = {
    var timer = System.currentTimeMillis()
    val simba = SimbaSession.builder().master(master)
      .appName("Tester")
      .config("simba.index.partitions", s"$partitions")
      .config("spark.sql.partition.method", "QuadTreePartitioner")
      .getOrCreate()
    logging("Starting session", timer)
    import simba.implicits._
    import simba.simbaImplicits._

    // Reading data...
    timer = System.currentTimeMillis()
    val points = simba.read.option("header", false).option("delimiter", "\t").schema(ST_Point_schema).csv(input).as[ST_Point]
    points.show(truncate = false)
    log("Dataset read", timer)

    // Testing distance join...
    timer = System.currentTimeMillis()
    val p1 = points.toDF("pid1","x1","y1","t1").index(HashMapType, "p1RT", Array("x1"))
    logger.info(s"P1 partitions: ${p1.rdd.getNumPartitions}")
    log("HashMap indexed", timer)
    timer = System.currentTimeMillis()
    val p2 = points.toDF("pid2","x2","y2","t2").index(RTreeType, "p2RT", Array("x2","y2"))
    logger.info(s"P2 partitions: ${p2.rdd.getNumPartitions}")
    log("RTree indexed", timer)

    // Setting parameters for partitioners...
    timer = System.currentTimeMillis()
    import org.apache.spark.sql.simba.index.{RTree, RTreeType}
    import org.apache.spark.sql.simba.spatial.{MBR, Point}
    import org.apache.spark.Partitioner
    import org.apache.spark.sql.simba.partitioner._

    val est_partitions       = 256
    val sample_rate          = 0.01
    val dimensions2          = 2
    val dimensions3          = 3
    val transfer_threshold   = 800 * 1024 * 1024
    val max_entries_per_node = 25
    val prePartitionerData2D = points.rdd.map(p => (new Point(Array(p.x, p.y)), p))
    val prePartitionerData3D = points.rdd.map(p => (new Point(Array(p.x, p.y, p.t)), p))
    log("Parameter set", timer)
    
    // Building 2D QuadTree partitioner...
    timer= System.currentTimeMillis()
    val quadtree2d = new QuadTreePartitioner(est_partitions, sample_rate, dimensions2, transfer_threshold, prePartitionerData2D)
    log("2D QuadTree built", timer)

    // Building 2D KDTree partitioner...
    timer= System.currentTimeMillis()
    val kdtree2d = new KDTreePartitioner(est_partitions, sample_rate, dimensions2, transfer_threshold, prePartitionerData2D)
    log("2D KDTree built", timer)

    // Building 2D RTree partitioner...
    timer= System.currentTimeMillis()
    val rtree2d = new STRPartitioner(est_partitions, sample_rate, dimensions2, transfer_threshold, max_entries_per_node, prePartitionerData2D)
    log("2D RTree built", timer)

    // Building 3D KDTree partitioner...
    timer= System.currentTimeMillis()
    val kdtree3d = new KDTreePartitioner(est_partitions, sample_rate, dimensions3, transfer_threshold, prePartitionerData3D)
    log("3D KDTree built", timer)

    // Building 3D RTree partitioner...
    timer= System.currentTimeMillis()
    val rtree3d = new STRPartitioner(est_partitions, sample_rate, dimensions3, transfer_threshold, max_entries_per_node, prePartitionerData3D)
    log("3D RTree built", timer)

    var pointsRDD = points.rdd
    logger.info(s"Number of partitions pre-partitioner: ${pointsRDD.getNumPartitions}")
    pointsRDD = prePartitionerData2D.partitionBy(kdtree2d).map(_._2)
    logger.info(s"Number of partitions post-kdtree 2D: ${pointsRDD.getNumPartitions}")
    pointsRDD = prePartitionerData2D.partitionBy(quadtree2d).map(_._2)
    logger.info(s"Number of partitions post-quadtree 2D: ${pointsRDD.getNumPartitions}")
    pointsRDD = prePartitionerData2D.partitionBy(rtree2d).map(_._2)
    logger.info(s"Number of partitions post-rtree 2D: ${pointsRDD.getNumPartitions}")

    simba.stop()
  }

  def log(msg: String, timer: Long, n: Long = 0, tag: String = ""): Unit ={
    if(n == 0)
      logger.info("%-50s|%6.2f".format(msg,(System.currentTimeMillis()-timer)/1000.0))
    else
      logger.info("%-50s|%6.2f|%6d|%s".format(msg,(System.currentTimeMillis()-timer)/1000.0,n,tag))
  }

  def logging(msg: String, timer: Long, n: Long = 0, tag: String = ""): Unit ={
    logger.info("%-50s | %6.2fs | %6d %s".format(msg, (System.currentTimeMillis() - timer)/1000.0, n, tag))
  }
}
