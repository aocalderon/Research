package edu.ucr.dblab

import org.slf4j.{LoggerFactory, Logger}
import org.rogach.scallop._
import scala.collection.JavaConverters._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, CircleRDD, PointRDD}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.enums.{GridType, IndexType}
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate, Point, Polygon}
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.index.SpatialIndex;
import edu.ucr.dblab.Utils._

object DistanceJoin{
  implicit val geofactory = new GeometryFactory()

  def envelope2polygon(e: Envelope): Polygon = {
    val minX = e.getMinX()
    val minY = e.getMinY()
    val maxX = e.getMaxX()
    val maxY = e.getMaxY()
    val p1 = new Coordinate(minX, minY)
    val p2 = new Coordinate(minX, maxY)
    val p3 = new Coordinate(maxX, maxY)
    val p4 = new Coordinate(maxX, minY)
    geofactory.createPolygon( Array(p1,p2,p3,p4,p1))
  }

  def join(leftRDD: PointRDD, rightRDD: PointRDD, d: Double): RDD[(Point, Vector[Point])] = {
    val circlesRDD = new CircleRDD(rightRDD, d)
    circlesRDD.analyze(rightRDD.boundary(), rightRDD.countWithoutDuplicates().toInt)
    circlesRDD.spatialPartitioning(leftRDD.getPartitioner)
    circlesRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

    val A = leftRDD.indexedRDD.rdd
    val B = circlesRDD.spatialPartitionedRDD.rdd
    val results = A.zipPartitions(B, preservesPartitioning = true){ (indexIt, circlesIt) =>
      var results = new scala.collection.mutable.ListBuffer[(Point, Point)]()
      if(!indexIt.hasNext || !circlesIt.hasNext){
        Vector.empty[(Point, Vector[Point])].toIterator
      } else {
        val index: SpatialIndex = indexIt.next()
        while(circlesIt.hasNext){
          val circle = circlesIt.next()
          val buffer = circle.getEnvelopeInternal
          val candidates = index.query(buffer)
          for( candidate <- candidates.asScala) {
            val center = candidate.asInstanceOf[Point]
            val x = circle.getCenterPoint.x - center.getX
            val y = circle.getCenterPoint.y - center.getY
            val x2 = x * x
            val y2 = y * y
            val dist = math.sqrt(x2 + y2)
            if(dist <= d){
              val point = geofactory.createPoint(circle.getCenterPoint)
              point.setUserData(circle.getUserData)
              results += ((center, point))
            }
          }
        }
        results.toVector.groupBy(_._1).map{ case(center, points) =>
          (center, points.map(_._2))
        }.toIterator
      }
    }

    results
  }

  def partitionBased(leftRDD: PointRDD, rightRDD: PointRDD, distance: Double, width: Double, grids: Vector[Envelope]): RDD[(List[(Int, Point)], List[String])] = {
    val circlesRDD = new CircleRDD(rightRDD, distance)
    circlesRDD.analyze(rightRDD.boundary(), rightRDD.countWithoutDuplicates().toInt)
    circlesRDD.spatialPartitioning(leftRDD.getPartitioner)
    circlesRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

    val A = leftRDD.spatialPartitionedRDD.rdd
    val B = circlesRDD.spatialPartitionedRDD.rdd
    val results = A.zipPartitions(B, preservesPartitioning = true){ (pointsIt, circlesIt) =>
      var results = new scala.collection.mutable.ListBuffer[(List[(Int, Point)], List[String])]()
      if(!pointsIt.hasNext || !circlesIt.hasNext){
        results.toIterator
      } else {
        val partition_id = TaskContext.getPartitionId
        val grid = grids(partition_id)
        //grid.expandBy(distance)
        val minX = grid.getMinX
        val minY = grid.getMinY
        val dX = grid.getMaxX - minX
        println(s"dX=$dX")
        val cols = math.ceil(dX / width).toInt
        println(s"cols=$cols")
        val dY = grid.getMaxY - minY
        val rows = math.ceil(dY / width).toInt

        val lgrids = for{
          i <- 0 to cols - 1
          j <- 0 to rows - 1
        } yield { 
          val p = envelope2polygon(new Envelope(minX + width * i, minX + width * (i + 1), minY + width * j, minY + width * (j + 1)))
          p.setUserData(s"${i + j * cols}")
          p
        }
        val slgrids = lgrids.map{ grid => s"${grid.toText()}\t${grid.getUserData.toString}\t${partition_id}\n" }.toList

        val tuples = pointsIt.toVector
          .map(p => (p.getX, p.getY, p))        
          .map{ case(x,y,p) => (x - minX, y - minY, p)} // just for testing...

        val points = tuples.map{ case(x, y, point) =>
          val i = math.floor(x / width).toInt
          val j = math.floor(y / width).toInt
          val id = i + j * cols

          ((id, point))
        }.toList

        results += ((points, slgrids))
        results.toIterator
      }
    }
    results
  }
}
