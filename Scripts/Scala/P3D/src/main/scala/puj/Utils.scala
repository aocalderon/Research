package puj

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.scala.Logging

import org.locationtech.jts.geom._

import scala.util.Random

object Utils extends Logging {

  // Case Classes...

  case class Data(oid: Int, tid: Int)

  case class SimplePartitioner(partitions: Int) extends Partitioner {
    override def numPartitions: Int          = partitions
    override def getPartition(key: Any): Int = key.asInstanceOf[Int]
  }

  // Methods...

  def gaussianSeries(
      n: Int = 1000,
      min: Double = 0.0,
      max: Double = 1.0
  ): List[Double] = {
    val data = (0 to n).toList.map(_ => Random.nextGaussian())
    rescaleList(data, min, max)
  }

  def rescaleList(
      data: List[Double],
      newMin: Double,
      newMax: Double
  ): List[Double] = {
    if (data.isEmpty) return List.empty
    val currentMin = data.min
    val currentMax = data.max
    if (currentMin == currentMax) {
      return data.map(_ => newMin)
    }
    data.map { x =>
      val normalized = (x - currentMin) / (currentMax - currentMin)
      normalized * (newMax - newMin) + newMin
    }
  }

  def generateGaussianPointset(
      n: Int,
      x_limit: Double = 5000.0,
      y_limit: Double = 5000.0,
      t_limit: Double = 1000.0
  )(implicit G: GeometryFactory, spark: SparkSession): RDD[Point] = {

    val Xs     = gaussianSeries(n, 0.0, x_limit)
    val Ys     = gaussianSeries(n, 0.0, y_limit)
    val Ts     = gaussianSeries(n, 0.0, t_limit).map(_.toInt)
    val points = (0 to n).map { i =>
      val x     = Xs(i)
      val y     = Ys(i)
      val t     = Ts(i)
      val point = G.createPoint(new Coordinate(x, y))
      point.setUserData(Data(i, t))
      point
    }

    saveAsTSV(
      "/tmp/P.wkt",
      points.map { point =>
        val x   = point.getX
        val y   = point.getY
        val t   = point.getUserData().asInstanceOf[Data].tid
        val i   = point.getUserData().asInstanceOf[Data].oid
        val wkt = point.toText()

        f"$i%d\t$x%.3f\t$y%.3f\t$t%d\t$wkt\n"
      }.toList
    )

    spark.sparkContext.parallelize(points)
  }

  def saveAsTSV(filename: String, content: Seq[String]): Unit = {
    import java.io._
    val pw = new PrintWriter(new File(filename))
    pw.write(content.mkString(""))
    pw.close()
    logger.info(s"Saved ${content.size} records to $filename")
  }
}
