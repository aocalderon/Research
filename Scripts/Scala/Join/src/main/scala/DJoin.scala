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

object DJoin{
  val model = new PrecisionModel(1000)
  val geofactory = new GeometryFactory(model)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DJoin")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()

    // Setting the distance...
    val distance = 5.0

    // Reading points dataset...
    val pointsTSV = "/Datasets/Demo/points.tsv"
    val offset = 1
    val splitter = FileDataSplitter.TSV
    val carry = true 
    val pointsRDD = new PointRDD(spark.sparkContext, pointsTSV, offset, splitter, carry)
    pointsRDD.analyze()
    val envelope = pointsRDD.boundary()
    envelope.expandBy(distance)
    pointsRDD.analyze(envelope, 100)

    // Reading centers dataset...
    val centersTSV = "/Datasets/Demo/centers.tsv"
    val centersRDD = new PointRDD(spark.sparkContext, centersTSV, offset, splitter, carry)
    centersRDD.analyze(envelope, 326)

    // Partitioning points...
    pointsRDD.spatialPartitioning(GridType.QUADTREE, 12)
    pointsRDD.spatialPartitionedRDD.cache

    println(s"Number of partitions in Points: ${pointsRDD.spatialPartitionedRDD.getNumPartitions}")

    // Saving points with its partition id...
    val pointsWKT = pointsRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex( (index, points) =>
      points.map{ point =>
        val id = point.getUserData().toString().split("\t")(0)
        s"${point.toText()}\t${id}\t${index}\n"
      }, preservesPartitioning = true).collect()
    val f1 = new PrintWriter("/tmp/points.wkt")
    f1.write(pointsWKT.mkString(""))
    f1.close()

    // Saving the MBRs for the points grid (just for reference)...
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

    // Creating buffers in centers and partitioning...
    val buffersRDD = new CircleRDD(centersRDD, distance)
    buffersRDD.analyze(envelope, 326)
    buffersRDD.spatialPartitioning(pointsRDD.getPartitioner)
    buffersRDD.spatialPartitionedRDD.cache

    // Saving centers with its partition id...
    val centersWKT = buffersRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex( (index, centers) =>
      centers.map{ center =>
        //val id = center.getUserData().toString().split("\t")(0)
        s"${center.getCenterGeometry.toText()}\t${index}\n"
      }, preservesPartitioning = true).collect()
    val f2 = new PrintWriter("/tmp/centers.wkt")
    f2.write(centersWKT.mkString(""))
    f2.close()

    // Performing the distance join ...
    val considerBoundary = true
    val usingIndex = false
    val results = JoinQuery
      .DistanceJoinQuery(pointsRDD, buffersRDD, usingIndex, considerBoundary)
      .rdd.map{ case(center, points) =>
        val arr = points.asScala.map(_.getUserData().toString().split("\t")(0))
          .map(_.toInt).toList.sorted
        s"${center.toText()}\t${arr.mkString(" ")}"
      }
    val n = results.count()

    //
    println(s"Number of partitions in Results: ${results.getNumPartitions}")
    val resultsWKT = results.mapPartitionsWithIndex( (index, wkts) =>
      wkts.map{ wkt =>
        s"${wkt}\t${index}\n"
      }, preservesPartitioning = true).collect()
    val f3 = new PrintWriter("/tmp/results.wkt")
    f3.write(resultsWKT.mkString(""))
    f3.close()

    spark.close()
  }
}

