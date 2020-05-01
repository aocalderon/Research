package edu.ucr.dblab.djoin

import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, CircleRDD}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.geometryObjects.Circle
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate, Point, Polygon, MultiPolygon}
import com.vividsolutions.jts.geom.GeometryFactory
import scala.collection.immutable.HashSet
import scala.collection.JavaConverters._
import scala.util.Random
import edu.ucr.dblab.Utils._
import edu.ucr.dblab.{StandardQuadTree, QuadRectangle}

object DistanceJoinV2 {
  implicit val geofactory = new GeometryFactory()

  case class UserData(data: String, isLeaf: Boolean)

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

  def join(leftRDD: SpatialRDD[Point],
    rightRDD: CircleRDD,
    usingIndex: Boolean = false,
    considerBoundary: Boolean = true): RDD[ (Point, Set[Point])] = {

    JoinQuery.DistanceJoinQueryFlat(leftRDD, rightRDD, usingIndex, considerBoundary).rdd
      .map{ case(left: Point, right: Geometry) =>
        (left, Set(right))
      }.reduceByKey{ (pids1, pids2) => pids1 ++ pids2 }
  }

  def partitionBasedQuadtreeV1(leftRDD: SpatialRDD[Point],
    rightRDD: CircleRDD,
    capacity: Int = 200,
    fraction: Double = 0.1,
    levels: Int = 5)
    (implicit global_grids: Vector[Envelope]): RDD[ (Point, Set[Point])] = {

    val left = leftRDD.spatialPartitionedRDD.rdd
    val right = rightRDD.spatialPartitionedRDD.rdd
    val distance = right.take(1).head.getRadius
    val results = left.zipPartitions(right, preservesPartitioning = true){ (pointsIt, circlesIt) =>
      var results = scala.collection.mutable.ListBuffer[ (Vector[ (Point, Set[Point]) ], String, String) ]()
      if(!pointsIt.hasNext || !circlesIt.hasNext){
        val pairs = Vector.empty[ (Point, HashSet[Point]) ]
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
        
        val pointsA = pointsIt.toVector.map{ a =>
          // Marking the left side...
          val userData = UserData(a.getUserData.toString(), true)
          a.setUserData(userData)
          a
        }
        val pointsB = circlesIt.toVector.map{ b =>
          val userData = UserData(b.getUserData.toString(), false)
          //val p = geofactory.createPoint(b.getCenterPoint)
          b.setUserData(userData)
          b
        }
        val nPointsA = pointsA.size
        val nPointsB = pointsB.size
        val points =  pointsA ++ pointsB
        val nPoints = points.size
        val quadtree = new StandardQuadTree[Geometry](grid, 0, capacity, levels)
        val timer1 = getTime(timer)

        // Feeding the quadtree A...
        timer = System.currentTimeMillis()
        pointsA.foreach { p =>
          quadtree.insert(new QuadRectangle(p.getEnvelopeInternal), p)
        }
        val timer2 = getTime(timer)

        // Feeding the quadtree B...
        timer = System.currentTimeMillis()
        pointsB.foreach { p =>
          quadtree.insert(new QuadRectangle(p.getEnvelopeInternal), p)
        }
        val timer3 = getTime(timer)

        // Report results...
        timer = System.currentTimeMillis()
        quadtree.assignPartitionIds
        val candidates = quadtree.getLeafZones.asScala.flatMap{ leaf =>
          val local_pid = leaf.partitionId
          val query = leaf.getEnvelope
          val points = quadtree
            .getElements(new QuadRectangle(query))
            //.getPointsByEnvelope(query)
            .asScala
            .map{ p =>
              val userData = p.getUserData.asInstanceOf[UserData]
              (p, userData.isLeaf)
            }
          val A = points.filter(_._2).map(_._1)
          val B = points.filterNot(_._2).map(_._1)
            .map{ circle =>
              val b = geofactory.createPoint(circle.asInstanceOf[Circle].getCenterPoint)
              b.setUserData(circle.getUserData)
              b
            }.filter{ b =>
              query.contains(b.getEnvelopeInternal)
            }

          val pairs = for{
            b <- B
            a <- A if a.distance(b) <= distance
          } yield {
            // Getting the original user data back...
            val aData = a.getUserData.asInstanceOf[UserData].data
            val bData = b.getUserData.asInstanceOf[UserData].data
            (b, bData, a, aData)
          }

          pairs
        }
        val pairs = candidates.map{ case(b, bData, a, aData) =>
            b.setUserData(bData)
            a.setUserData(aData)
            (b, Set(a.asInstanceOf[Point]))
        }.groupBy(_._1).toVector.map{ case(b, as) => (b, as.flatMap(_._2).toSet) }
        val timer4 = getTime(timer)
        
        val stats = f"${envelope2polygon(gridEnvelope).toText()}\t" +
        f"${global_gid}\t${nPointsA}\t${nPointsB}\t" +
        f"${timer1}\t${timer2}\t${timer3}\t${timer4}\n"
        val lgridsWKT = quadtree.getLeafZones.asScala.map{ leaf =>
          s"${envelope2polygon(leaf.getEnvelope).toText()}\t${leaf.partitionId}\n"
        }.mkString("")
        results += ((pairs, stats, lgridsWKT))
        results.toIterator
      }
    }
    val pairs = results.flatMap(_._1).reduceByKey(_ ++ _)

    pairs
  }    

  def partitionBasedQuadtreeV2(leftRDD: SpatialRDD[Point],
    rightRDD: CircleRDD,
    capacity: Int = 200,
    fraction: Double = 0.1,
    levels: Int = 5)
    (implicit global_grids: Vector[Envelope]): RDD[ (Point, Set[Point])] = {

    val left = leftRDD.spatialPartitionedRDD.rdd
    val right = rightRDD.spatialPartitionedRDD.rdd
    val distance = right.take(1).head.getRadius
    val results = left.zipPartitions(right, preservesPartitioning = true){ (pointsIt, circlesIt) =>
      var results = scala.collection.mutable.ListBuffer[ (Vector[ (Point, Set[Point]) ], String, String) ]()
      if(!pointsIt.hasNext || !circlesIt.hasNext){
        val pairs = Vector.empty[ (Point, HashSet[Point]) ]
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

        A.map(a => s"${a._2.toText()}\t$global_gid\t${a._1}\t${a._2.getUserData}").foreach { println }

        // Feeding B...
        timer = System.currentTimeMillis()
        val B = circlesB.flatMap{ circle =>
          val query = new QuadRectangle(circle.getEnvelopeInternal) 
          quadtree.findZones(query).asScala
            .map{ zone =>
              (zone.partitionId.toInt, envelope2polygon(circle.getEnvelopeInternal).getCentroid)
            }
        }
        val timer3 = getTime(timer)

        B.map(b => s"${b._2.toText()}\t$global_gid\t${b._1}").foreach { println }

        // Report results...
        timer = System.currentTimeMillis()
        val candidates = for{
          a <- A
          b <- B if a._1 == b._1 && a._2.distance(b._2) <= distance
        } yield {
          (b._2, Set(a._2))
        }
        val pairs = candidates.groupBy(_._1).toVector
          .map{ case(p, pairs) => (p, pairs.flatMap(_._2).toSet)}
        //val pairs = Vector.empty[ (Point, HashSet[Point]) ]
        val timer4 = getTime(timer)
        
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
    val pairs = results.flatMap(_._1).reduceByKey(_ ++ _)

    pairs
  }    
}

/*
 save(s"/tmp/edgesStats_${appId}.wkt"){
 partitionBased1.mapPartitionsWithIndex(
 {case(index, iter) =>
 iter.map{ case(pairs, stats, lgrids) => s"$stats\t$appId\n" }
 }, preservesPartitioning = true).collect().sorted
 }
 save(s"/tmp/edgesLGrids.wkt"){
 partitionBased1.mapPartitionsWithIndex(
 {case(index, iter) =>
 iter.map{ case(pairs, stats, lgrids) => s"$lgrids" }
 }, preservesPartitioning = true).collect()
 }
 */

