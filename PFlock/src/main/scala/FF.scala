import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.rdd.RDD

object FF{
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val ST_Point_schema = ScalaReflection.schemaFor[ST_Point].dataType.asInstanceOf[StructType]
  case class ST_Point(pid: Int, x: Double, y: Double, t: Double)
  
  def main(args: Array[String]) {
    val conf       = new ConfFF(args)
    val input      = conf.input()
    val debug      = conf.debug()
    val master     = conf.master()
    val partitions = conf.partitions()
    val cores      = conf.cores()
    val x_delta    = conf.grain_x()
    val y_delta    = conf.grain_y()
    val t_delta    = conf.grain_t()

    logger.info("Logs started")

    // Starting session...
    var timer = System.currentTimeMillis()
    val simba = SimbaSession.builder().master(master).appName("FF")
      .config("simba.index.partitions", partitions)
      .config("spark.cores.max", cores)
      .getOrCreate()
    import simba.implicits._
    import simba.simbaImplicits._
    log("Session started", timer)

    // Indexing points...
    timer = System.currentTimeMillis()
    val points = simba.read.option("delimiter", "\t").option("header", "false").schema(ST_Point_schema).csv(input).as[ST_Point].cache()
    val nPoints = points.count()
    log("Points indexed", timer, nPoints, "points")
    if(debug) points.show(truncate = false)

    // Computing the grid...
    timer = System.currentTimeMillis()
    val bounds = points.agg(min($"x").as("min_x"), max($"x").as("max_x"), min($"y").as("min_y"), max($"y").as("max_y"), min($"t").as("min_t"), max($"t").as("max_t")).collect().map(s => (s.getDouble(0),s.getDouble(1),s.getDouble(2),s.getDouble(3),s.getDouble(4),s.getDouble(5))).head
    if(debug) logger.info(s"Bounds: ${bounds}")
    val cells = ((bounds._1 / x_delta).toInt to (bounds._2 / x_delta).toInt).toList
      .cross(((bounds._3 / y_delta).toInt to (bounds._4 / y_delta).toInt)).toList
      .cross(((bounds._5 / t_delta).toInt to (bounds._6 / t_delta).toInt)).toList.zipWithIndex
    val grid = simba.createDataset(cells)
      .map(c => (c._1._1._1,c._1._1._2,c._1._2,c._2))
      .toDF("x","y","t","gid").as("grid")
      .cache()
    val nGrid = grid.count().toInt
    if(debug) grid.orderBy($"gid").show(truncate=false)
    log("Grid computed", timer, nGrid, "cells")

    // Indexing points by grid...
    timer = System.currentTimeMillis()
    val pointsByGrid = points.map{ p =>
        val x_prime = (p.x / x_delta).toInt
        val y_prime = (p.y / y_delta).toInt
        val t_prime = (p.t / t_delta).toInt
        (p.pid, p.x, p.y, p.t, x_prime, y_prime, t_prime)
      }
      .toDF("pid", "x", "y", "t", "x_prime", "y_prime", "t_prime")
      .as("points")
    val gridPoints = pointsByGrid.join(grid, col("points.x_prime") === col("grid.x") && col("points.y_prime") === col("grid.y") && col("points.t_prime") === col("grid.t"), "left")
      .map(p => (p.getInt(10), ST_Point(p.getInt(0), p.getDouble(1), p.getDouble(2), p.getDouble(3))))
      .rdd
      .partitionBy(new GridPartitioner(nGrid))
      .map(_._2)
    val nGridPoints = gridPoints.count()
    log("Points by grid indexed", timer, nGridPoints, "points")

    if(debug) logger.info(s"Number of partitions: ${gridPoints.getNumPartitions}")

    if(debug){
      val p = gridPoints.mapPartitionsWithIndex{ (index, data) =>
        data.map(d => s"$index,${d.x},${d.y},${d.t}\n")
      }
      savePartitions(p, "/tmp/p.csv")
    }

    // Stopping session...
    simba.stop()
    logger.info("Session closed")
  }

  def d(p1: ST_Point, p2: ST_Point): Double = {
    scala.math.sqrt(scala.math.pow(p1.x - p2.x, 2.0) + scala.math.pow(p1.y - p2.y, 2.0) + scala.math.pow(p1.t - p2.t, 2.0))
  }

  def log(msg: String, timer: Long, n: Long = 0, tag: String = ""): Unit ={
    if(n == 0)
      logger.info("%-50s|%6.2f".format(msg,(System.currentTimeMillis()-timer)/1000.0))
    else
      logger.info("%-50s|%6.2f|%6d|%s".format(msg,(System.currentTimeMillis()-timer)/1000.0,n,tag))
  }

  import java.io._
  def savePartitions(data: RDD[String], filename: String): Unit ={
    val pw = new PrintWriter(new File(filename))
    pw.write(data.collect().mkString(""))
    pw.close
  }

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }

  import org.apache.spark.Partitioner

  class GridPartitioner(partitions: Int) extends Partitioner{
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      key.asInstanceOf[Int]
    }
  }

}

import org.rogach.scallop.{ScallopConf, ScallopOption}

class ConfFF(args: Seq[String]) extends ScallopConf(args) {
  val input:   ScallopOption[String]   =  opt[String]   (required = true)
  val master:  ScallopOption[String]   =  opt[String]   (default = Some("spark://169.235.27.134:7077"))
  val partitions: ScallopOption[Int]   =  opt[Int]      (default = Some(128))
  val cores:   ScallopOption[Int]      =  opt[Int]      (default = Some(21))
  val grain_x: ScallopOption[Double]   =  opt[Double]   (default = Some(10000.0))
  val grain_y: ScallopOption[Double]   =  opt[Double]   (default = Some(10000.0))
  val grain_t: ScallopOption[Double]   =  opt[Double]   (default = Some(3.0))
  val debug:   ScallopOption[Boolean]  =  opt[Boolean]  (default = Some(false))

  verify()
}
