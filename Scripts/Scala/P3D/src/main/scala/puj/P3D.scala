package puj

import edu.ucr.dblab.pflock.sedona.quadtree.{QuadRectangle, StandardQuadTree}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom._
import edu.ucr.dblab.pflock.sedona.quadtree.Quadtree

import scala.collection.JavaConverters._

object P3D {
    case class Data(oid: Int, tid: Int)

    def main(args: Array[String]): Unit = {
        implicit val params = new Params(args)
        implicit val G = new GeometryFactory(new PrecisionModel(1.0 / params.tolerance()))

        implicit val spark: SparkSession = SparkSession.builder()
            .config("spark.serializer", classOf[KryoSerializer].getName)
            .master(params.master())
            .appName("P3D").getOrCreate()

        val pointsRDD = spark.read
            .option("header", value = false)
            .option("delimiter", "\t")
            .csv(params.filename)
            .rdd
            .map{ row =>
                val oid = row.getString(0).toInt
                val lon = row.getString(1).toDouble
                val lat = row.getString(2).toDouble
                val tid = row.getString(3).toInt

                val point = G.createPoint(new Coordinate(lon, lat))
                point.setUserData(Data(oid, tid))
                point
            }.cache()
        val n = pointsRDD.count()

        val (quadtree, cells, universe) = Quadtree.build(pointsRDD, new Envelope(), capacity = 50, fraction = 0.5)
        quadtree.values.map{ cell =>
                G.toGeometry(cell).toText()
            }.foreach(println)

        spark.close
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