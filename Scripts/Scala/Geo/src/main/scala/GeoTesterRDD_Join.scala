package edu.ucr.dblab

import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, CircleRDD, PointRDD}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.enums.{GridType, IndexType, FileDataSplitter}
import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, Coordinate, Point}
import scala.collection.JavaConverters._
import java.io.PrintWriter

object GeoTesterRDD_Join{
  val model = new PrecisionModel(1000)
  val geofactory = new GeometryFactory(model)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("GeoTester_Join")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    val data = "/Datasets/Demo/points.tsv"
    val offset = 1
    val splitter = FileDataSplitter.TSV
    val carry = true 
    val pointsRDD = new PointRDD(spark.sparkContext, data, offset, splitter, carry)
    pointsRDD.analyze()

    val distance = 10.0

    pointsRDD.spatialPartitioning(GridType.QUADTREE, 10)
    pointsRDD.spatialPartitionedRDD.cache

    //
    println(s"Number of partitions in Points: ${pointsRDD.spatialPartitionedRDD.getNumPartitions}")
    val pointsWKT = pointsRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex( (index, points) =>
      points.map{ point =>
        s"${point.toText()}\t${index}\n"
      }, preservesPartitioning = true).collect()
    val f1 = new PrintWriter("/tmp/points.wkt")
    f1.write(pointsWKT.mkString(""))
    f1.close()
    
    val buffersRDD = new CircleRDD(pointsRDD, distance)
    buffersRDD.analyze()
    buffersRDD.spatialPartitioning(pointsRDD.getPartitioner)
    buffersRDD.spatialPartitionedRDD.cache

    

    val gridsWKT = pointsRDD.partitionTree.getLeafZones.asScala.map{ z =>
      val id = z.partitionId
      val e = z.getEnvelope
      val (x1,x2,y1,y2) = (e.getMinX, e.getMaxX, e.getMinY, e.getMaxY)
      val p1 = new Coordinate(x1, y1)
      val p2 = new Coordinate(x2, y1)
      val p3 = new Coordinate(x2, y2)
      val p4 = new Coordinate(x1, y2)
      val p = geofactory.createPolygon(Array(p1,p2,p3,p4,p1))
      s"${p.toText()}\t${id}\n"
    }
    val g = new PrintWriter("/tmp/grids.wkt")
    g.write(gridsWKT.mkString(""))
    g.close

    // Finding pairs and centers...
    def calculateCenterCoordinates(p1: Point, p2: Point, r2: Double): (Point, Point) = {
      val X: Double = p1.getX - p2.getX
      val Y: Double = p1.getY - p2.getY
      val D2: Double = math.pow(X, 2) + math.pow(Y, 2)
      if (D2 != 0.0){
        val root: Double = math.sqrt(math.abs(4.0 * (r2 / D2) - 1.0))
        val h1: Double = ((X + Y * root) / 2) + p2.getX
        val k1: Double = ((Y - X * root) / 2) + p2.getY
        val h2: Double = ((X - Y * root) / 2) + p2.getX
        val k2: Double = ((Y + X * root) / 2) + p2.getY
        val h = geofactory.createPoint(new Coordinate(h1,k1))
        val k = geofactory.createPoint(new Coordinate(h2,k2))
        (h, k)
      } else {
        val h = geofactory.createPoint(new Coordinate(-1.0, -1.0))
        val k = geofactory.createPoint(new Coordinate(-1.0, -1.0))
        (h, k)
      }
    }

    val r2: Double = math.pow(distance / 2.0, 2)
    val considerBoundary = true
    val usingIndex = false
    val centersPairs = JoinQuery
      .DistanceJoinQueryFlat(pointsRDD, buffersRDD, usingIndex, considerBoundary)
      .rdd.map{ pair =>
        val id1 = pair._1.getUserData().toString().split("\t").head.trim().toInt
        val p1  = pair._1.getCentroid
        val id2 = pair._2.getUserData().toString().split("\t").head.trim().toInt
        val p2  = pair._2
        ( (id1, p1) , (id2, p2) )
      }.filter(p => p._1._1 < p._2._1).map{ p =>
        val p1 = p._1._2
        val p2 = p._2._2
        calculateCenterCoordinates(p1, p2, r2)
      }

    val centersRDD = centersPairs.map(_._1)
      .union(centersPairs.map(_._2))
      .cache()
    val nCenters = centersRDD.count()

    //
    println(s"Number of partitions in Centers: ${centersRDD.getNumPartitions}")
    val centersWKT = centersRDD.mapPartitionsWithIndex( (index, centers) =>
      centers.map{ center =>
        s"${center.toText()}\t${index}\n"
      }, preservesPartitioning = true).collect()
    val f2 = new PrintWriter("/tmp/centers.wkt")
    f2.write(centersWKT.mkString(""))
    f2.close()

    val centersWKT2 = centersRDD.zipWithIndex.map{ case(center, id) =>
      s"${id}\t${center.getX}\t${center.getY}\n"
    }.collect()
    val f3 = new PrintWriter("/tmp/centers.tsv")
    f3.write(centersWKT2.mkString(""))
    f3.close()

    spark.close()
  }
}

