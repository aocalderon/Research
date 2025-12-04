package puj

import edu.ucr.dblab.pflock.sedona.quadtree.{QuadRectangle, StandardQuadTree, Quadtree}

import org.apache.spark.{Partitioner, TaskContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.logging.log4j.LogManager

import org.locationtech.jts.geom._
import org.locationtech.jts.index.strtree.GeometryItemDistance

import scala.collection.JavaConverters._
import scala.util.Random

object P3D {
    private val logger = LogManager.getLogger("MyLogger")
    
    case class Data(oid: Int, tid: Int)

    def main(args: Array[String]): Unit = {
        logger.info("Starting P3D application")
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
        val n: Int = 100000
        val sd: Double = 250.0
        val x_limit: Double = 1000.0
        val y_limit: Double = 1000.0
        val t_limit: Int = 100
        val Xs = gaussianSeries(n, n / 2.0, sd)
        val Ys = gaussianSeries(n, n / 2.0, sd)
        val Ts = gaussianSeries(n, 50, 25)
        val points = Xs.zip(Ys).zipWithIndex.map{ case(tuple, oid) =>
                val point = G.createPoint(new Coordinate(tuple._1, tuple._2))
                point.setUserData(Data(oid, Math.floor(Ts(oid)).toInt))
                point
            }
        saveAsTSV("/tmp/P.wkt",
            points.map{ point =>
                point.toText() + "\n"
            }.toList
        )
        val pointsRDD: RDD[Point] = spark.sparkContext.parallelize(points)

        val (quadtree, cells, universe) = Quadtree.build(pointsRDD, new Envelope(), capacity = 50, fraction = 0.5)

        saveAsTSV("/tmp/Q.wkt",
            quadtree.values.map{ cell =>
                G.toGeometry(cell).toText() + "\n"
            }.toList
        )

        val pointsSRDD = pointsRDD.mapPartitions{ iter => 
            iter.map{ point =>
                val cid = cells.query(point.getEnvelopeInternal()).asScala.head.asInstanceOf[Int]
                (cid, point)
            }
        }.partitionBy(SimplePartitioner(quadtree.size)).map(_._2).cache()

        val count = pointsSRDD.count()
        logger.info(s"Total points after partitioning: $count")

        pointsSRDD.mapPartitions{ it =>
            val partitionId = TaskContext.getPartitionId()
            it.map{ point =>
                point.getUserData().asInstanceOf[Data].tid
            }.toList.groupBy(tid => tid).map{ case(tid, list) =>
                (tid, list.size)
            }.toIterator
        }.groupByKey().map{ case(tid, counts) =>
            val total = counts.sum
            (tid, total)
        }.collect().foreach{ case(tid, total) =>
            println(s"Type $tid has total $total points")
        }

        spark.close
        logger.info("SparkSession closed")
    }    

    def gaussianSeries(size: Int = 1000, mean: Double = 500.0, stdDev: Double = 150): List[Double] = List.fill(size) {
        // nextGaussian() generates numbers with mean=0.0 and stdDev=1.0
        // Scale and shift the result
        mean + stdDev * Random.nextGaussian()
    }

    def saveAsTSV(filename: String, content: Seq[String]): Unit = {
        import java.io._
        val pw = new PrintWriter(new File(filename))
        pw.write(content.mkString(""))
        pw.close()
    }

    case class SimplePartitioner(partitions: Int) extends Partitioner {
        override def numPartitions: Int = partitions
        override def getPartition(key: Any): Int = key.asInstanceOf[Int]
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