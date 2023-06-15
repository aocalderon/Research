package edu.ucr.dblab.pflock

import com.vividsolutions.jts.geom.{GeometryFactory, Coordinate, Point}
import org.apache.spark.sql.SparkSession
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.slf4j.Logger

import quadtree._
import Utils._

object MF_Utils {

  def loadCachedData[T](implicit spark: SparkSession, S: Settings,
    geofactory: GeometryFactory, logger: Logger): (RDD[Point], StandardQuadTree[T], Map[Int, Cell]) = {

    val ((quadtree, cells), tIndex) = timer{
      val filename = getOutputName(S.input, "quadtree", "wkt")
      Quadtree.loadQuadtreeAndCells[T](filename)
    }
    val nIndex = cells.size

    val ( (pointsRaw, nRead), tRead) = timer{
      val pointsRaw = spark.read
        .option("delimiter", "\t")
        .option("header", false)
        .textFile(S.input).rdd
        .map { line =>
          val arr = line.split("\t")
          val i = arr(0).toInt
          val x = arr(1).toDouble
          val y = arr(2).toDouble
          val t = arr(3).toInt
          val c = arr(4).toInt
          val point = geofactory.createPoint(new Coordinate(x, y))
          point.setUserData(Data(i, t))
          (c, point)
        }.partitionBy(new Partitioner {
          def numPartitions: Int = cells.size
          def getPartition(key: Any): Int = key.asInstanceOf[Int]
        }).cache
        .map(_._2).cache
      val nRead = pointsRaw.count
      (pointsRaw, nRead)
    }
    log(s"Read|$nRead")
    logt(s"Read|$tRead")

    log(s"Index|$nIndex")
    logt(s"Index|$tIndex")

    (pointsRaw, quadtree, cells)
  }

  def getOutputName(output_base: String, prefix: String = "", ext: String = "wkt")
    (implicit S: Settings): String = {

    val home = System.getenv("HOME")
    val git_path = "/Research/local_path"
    val dataset = S.dataset
    val hdfs_path = output_base.split("/").reverse.tail.reverse.mkString("/")
    val E = S.epsilon.toInt
    val M = S.mu

    s"${home}${git_path}/${hdfs_path}/${prefix}${dataset}_E${E}_M${M}.${ext}"
  }
}
