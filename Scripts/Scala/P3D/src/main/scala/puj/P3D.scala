package puj

import edu.ucr.dblab.pflock.sedona.quadtree.{QuadRectangle, StandardQuadTree}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom._
import edu.ucr.dblab.pflock.sedona.quadtree.Quadtree

import scala.collection.JavaConverters._
import scala.util.Random

object P3D {
    case class Data(oid: Int, tid: Int)

    def main(args: Array[String]): Unit = {
        implicit val params = new Params(args)
        implicit val G = new GeometryFactory(new PrecisionModel(1.0 / params.tolerance()))

        implicit val spark: SparkSession = SparkSession.builder()
            .config("spark.serializer", classOf[KryoSerializer].getName)
            .master(params.master())
            .appName("P3D").getOrCreate()

        // val pointsRDD = spark.read
        //     .option("header", value = false)
        //     .option("delimiter", "\t")
        //     .csv(params.filename)
        //     .rdd
        //     .map{ row =>
        //         val oid = row.getString(0).toInt
        //         val lon = row.getString(1).toDouble
        //         val lat = row.getString(2).toDouble
        //         val tid = row.getString(3).toInt

        //         val point = G.createPoint(new Coordinate(lon, lat))
        //         point.setUserData(Data(oid, tid))
        //         point
        //     }.cache()
        // val n = pointsRDD.count()
        val n: Int = 10000
        val sd: Double = 250.0
        val x_limit: Double = 1000.0
        val y_limit: Double = 1000.0
        val t_limit: Int = 10
        val Xs = gaussianSeries(n, n / 2.0, sd)
        val Ys = gaussianSeries(n, n / 2.0, sd)
        val points = Xs.zip(Ys).zipWithIndex.map{ case(tuple, oid) =>
                val tid = Random.nextInt(t_limit)
                val point = G.createPoint(new Coordinate(tuple._1, tuple._2))
                point.setUserData(Data(oid, tid))
                point
            }
        val pointsRDD = spark.sparkContext.parallelize(points)

        val (quadtree, cells, universe) = Quadtree.build(pointsRDD, new Envelope(), capacity = 50, fraction = 0.5)
        quadtree.values.map{ cell =>
                G.toGeometry(cell).toText()
            }.foreach(println)

        spark.close
    }    

    def gaussianSeries(size: Int = 1000, mean: Double = 500.0, stdDev: Double = 150): List[Double] = List.fill(size) {
        // nextGaussian() generates numbers with mean=0.0 and stdDev=1.0
        // Scale and shift the result
        mean + stdDev * Random.nextGaussian()
    }
}

import org.rogach.scallop._

class Params(args: Seq[String]) extends ScallopConf(args) {
  val filename = "/opt/Research/Datasets/dense2.tsv"
  val input:     ScallopOption[String]  = opt[String]  (default = Some(filename))
  val master:    ScallopOption[String]  = opt[String]  (default = Some("local[3]"))
  val epsilon:   ScallopOption[Double]  = opt[Double]  (default = Some(10.0))
  val mu      :  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val tolerance: ScallopOption[Double]  = opt[Double]  (default = Some(1e-3))

  verify()
}