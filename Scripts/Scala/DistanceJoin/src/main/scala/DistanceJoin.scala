package edu.ucr.dblab.djoin

import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, CircleRDD}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.enums.IndexType
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate, Point, Polygon, MultiPolygon}
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.index.SpatialIndex
import scala.collection.immutable.HashSet
import scala.collection.JavaConverters._
import scala.util.Random
import edu.ucr.dblab.Utils._

object DistanceJoin {
  val scale = 1000.0
  val model = new PrecisionModel(scale)
  val geofactory = new GeometryFactory(model)

  case class UserData(data: String, isLeaf: Boolean)

  def join(leftRDD: SpatialRDD[Point],
    rightRDD: CircleRDD,
    usingIndex: Boolean = false,
    considerBoundary: Boolean = true): RDD[(Point, Point)] = {

    JoinQuery.DistanceJoinQueryFlat(leftRDD, rightRDD, usingIndex, considerBoundary).rdd
      .map{ case(geometry, point) =>
        (geometry.asInstanceOf[Point], point)
      }
  }

  def indexBased(leftRDD: SpatialRDD[Point],
    rightRDD: CircleRDD,
    buildOnSpatialPartitionedRDD: Boolean = true)
    (implicit global_grids: Vector[Envelope]): RDD[ (Point, Point)] = {

    leftRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)
    leftRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)

    val left = leftRDD.indexedRDD.rdd
    val right = rightRDD.spatialPartitionedRDD.rdd
    val distance = right.take(1).head.getRadius
    val results = left.zipPartitions(right, preservesPartitioning = true){ (indexIt, circlesIt) =>
      var results = new scala.collection.mutable.ListBuffer[(Point, Point)]()
      if(!indexIt.hasNext || !circlesIt.hasNext){
        List.empty[(Point, Point)].toIterator
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
            if(dist <= distance){
              val point = circle2point(circle)
              results += ((point, center))
            }
          }
        }
        results.toIterator
        }
      }
    results
  }

  def partitionBased(leftRDD: SpatialRDD[Point],
    rightRDD: CircleRDD,
    capacity: Int = 200,
    fraction: Double = 0.1,
    levels: Int = 5)
    (implicit global_grids: Vector[Envelope]): RDD[ (Point, Point)] = {

    val left = leftRDD.spatialPartitionedRDD.rdd
    val right = rightRDD.spatialPartitionedRDD.rdd
    val distance = right.take(1).head.getRadius
    val results = left.zipPartitions(right, preservesPartitioning = true){ (pointsIt, circlesIt) =>
      var results = scala.collection.mutable.ListBuffer[ (Vector[(Point, Point)], String, String) ]()
      if(!pointsIt.hasNext || !circlesIt.hasNext){
        val pairs = Vector.empty[ (Point, Point) ]
        val global_gid = TaskContext.getPartitionId
        val gridEnvelope = global_grids(global_gid)
        val stats = f"${envelope2polygon(gridEnvelope).toText()}\t" +
        f"${global_gid}\t${0}\t${0}\t" +
        f"${0}\t${0}\t${0}\t${0}\n"
        results += ((pairs, stats, ""))
        results.toIterator
      } else {
        // Building the local quadtree...
        var timer = System.currentTimeMillis()
        val global_gid = TaskContext.getPartitionId
        val gridEnvelope = global_grids(global_gid)
        gridEnvelope.expandBy(distance)
        val grid = new QuadRectangle(gridEnvelope)

        val pointsA = pointsIt.toVector
        val circlesB = circlesIt.toVector
        val nPointsA = pointsA.size
        val nCirclesB = circlesB.size

        val quadtree = new StandardQuadTree[Int](grid, 0, capacity, levels)
        val sampleSize = (fraction * nPointsA).toInt
        val sample = Random.shuffle(pointsA).take(sampleSize)
        sample.foreach { p =>
          quadtree.insert(new QuadRectangle(p.getEnvelopeInternal), 1)
        }
        quadtree.assignPartitionIds()
        val timer1 = getTime(timer)

        // Feeding A...
        timer = System.currentTimeMillis()
        val A = pointsA.flatMap{ point =>
          val query = new QuadRectangle(point.getEnvelopeInternal) 
          quadtree.findZones(query).asScala
            .map{ zone =>
              (zone.partitionId.toInt, point)
            }
        }
        val timer2 = getTime(timer)

        // Feeding B...
        timer = System.currentTimeMillis()
        val B = circlesB.flatMap{ circle =>
          val query = new QuadRectangle(circle.getEnvelopeInternal) 
          quadtree.findZones(query).asScala
            .map{ zone =>
              val center = circle2point(circle)
              (zone.partitionId.toInt, center, zone.getEnvelope)
            }
        }
        val timer3 = getTime(timer)

        //
        //A.map(b => s"${b._2.toText()}\t${b._2.getUserData.toString}\t${b._1}")
        //  .foreach{ println }
        //B.map(b => s"${b._2.toText()}\t${b._2.getUserData.toString}\t${b._1}")
        //  .foreach{ println }

        // Report results...
        timer = System.currentTimeMillis()
        val pairs = for{
          a <- A
          b <- B if a._1 == b._1 && a._2.distance(b._2) <= distance 
        } yield {
          
          (b._2, a._2)
        }
        val timer4 = getTime(timer)

        //
        //pairs
        // .map{ case(b, a) => s"${b.toText()}\t${b.getUserData}\t${a.toText()}\t${a.getUserData}" }
        // .foreach{ println }
        
        val stats = f"${envelope2polygon(gridEnvelope).toText()}\t" +
        f"${global_gid}\t${nPointsA}\t${nCirclesB}\t" +
        f"${timer1}\t${timer2}\t${timer3}\t${timer4}\n"
        val lgridsWKT = quadtree.getLeafZones.asScala.map{ leaf =>
          s"${envelope2polygon(leaf.getEnvelope).toText()}\t${leaf.partitionId}\n"
        }.mkString("")
        results += ((pairs, stats, lgridsWKT))
        results.toIterator
      }
    }
    val pairs = results.flatMap(_._1)
    pairs
  }    

  def getTime(time: Long): Long = { (System.currentTimeMillis() - time ) }

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

  def round(number: Double): Double = Math.round(number * scale) / scale;

  def groupByLeftPoint(pairs: RDD[(Point, Point)]): RDD[(Point, Vector[Point])] = {
    pairs.map{ case(l, r) =>
      (l, Vector(r))
    }.reduceByKey {_ ++ _}
  }

  def groupByRightPoint(pairs: RDD[(Point, Point)]): RDD[(Point, Vector[Point])] = {
    pairs.map{ case(l, r) =>
      (r, Vector(l))
    }.reduceByKey {_ ++ _}
  }

  def circle2point(circle: org.datasyslab.geospark.geometryObjects.Circle): Point = {
    val point = geofactory.createPoint(circle.getCenterPoint)
    point.setUserData(circle.getUserData)
    point
  }
}

