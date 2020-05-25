package edu.ucr.dblab.djoin

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.TaskContext
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, CircleRDD}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.enums.IndexType
import org.datasyslab.geospark.geometryObjects.Circle
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate, Point, Polygon, MultiPolygon}
import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.index.SpatialIndex
import scala.collection.immutable.HashSet
import scala.collection.JavaConverters._
import scala.util.Random
import edu.ucr.dblab.Utils._

case class PointLGrid(point: Point, lgrid: Int)

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

  def baseline(leftRDD: SpatialRDD[Point],
    rightRDD: CircleRDD)
    (implicit global_grids: Vector[Envelope]): RDD[ (Point, Point) ] = {

    val left = leftRDD.spatialPartitionedRDD.rdd
    val right = rightRDD.spatialPartitionedRDD.rdd
    val distance = right.take(1).head.getRadius

    left.zipPartitions(right, preservesPartitioning = true){ (pointsIt, circlesIt) =>
      if(!pointsIt.hasNext || !circlesIt.hasNext){
        List.empty[(Point, Point)].toIterator
      } else {
        val A = pointsIt.toVector
        val B = circlesIt.toVector.map(circle2point)
        val pairs = for{
          a <- A
          b <- B if a.distance(b) <= distance
        } yield {
          (b, a)
        }
        pairs.toIterator
      }
    }
  }

  def baselineDebug(leftRDD: SpatialRDD[Point],
    rightRDD: CircleRDD)
    (implicit global_grids: Vector[Envelope]): RDD[ (Point, Point) ] = {

    val left = leftRDD.spatialPartitionedRDD.rdd
    val right = rightRDD.spatialPartitionedRDD.rdd
    val distance = right.take(1).head.getRadius

    //
    val appId = SparkSession.builder().getOrCreate().sparkContext.applicationId

    left.zipPartitions(right, preservesPartitioning = true){ (pointsIt, circlesIt) =>
      if(!pointsIt.hasNext || !circlesIt.hasNext){
        List.empty[(Point, Point)].toIterator
      } else {
        val A = pointsIt.toVector
        val B = circlesIt.toVector.map(circle2point)
        val pairs = for{
          a <- A
          b <- B if a.distance(b) <= distance
        } yield {
          (b, a)
        }

        //
        val global_gid = TaskContext.getPartitionId
        val local_gid = 0
        val nA = A.size
        val nB = B.size
        val ops = nA * nB
        logger.info(s"DEBUG|Baseline|$global_gid|$local_gid|$nA|$nB|$ops|$appId")

        pairs.toIterator
      }
    }
  }

  def indexBased(leftRDD: SpatialRDD[Point],
    rightRDD: CircleRDD,
    buildOnSpatialPartitionedRDD: Boolean = true)
    (implicit global_grids: Vector[Envelope]): RDD[ (Point, Point) ] = {

    leftRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)
    leftRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)

    val left = leftRDD.indexedRDD.rdd
    val right = rightRDD.spatialPartitionedRDD.rdd
    val distance = right.take(1).head.getRadius
    left.zipPartitions(right, preservesPartitioning = true){ (indexIt, circlesIt) =>
      if(!indexIt.hasNext || !circlesIt.hasNext){
        List.empty[(Point, Point)].toIterator
      } else {
        val index: SpatialIndex = indexIt.next()
        val B = circlesIt
        val pairs = for{
          b <- B
          a <- index.query(b.getEnvelopeInternal).asScala.map(_.asInstanceOf[Point])
          if isWithin(a, b)
        } yield{
          (circle2point(b), a)
        }
        pairs.toIterator
      }
    }
  }

  def indexBasedDebug(leftRDD: SpatialRDD[Point],
    rightRDD: CircleRDD,
    buildOnSpatialPartitionedRDD: Boolean = true)
    (implicit global_grids: Vector[Envelope]): RDD[ (Point, Point) ] = {
    
    //
    val appId = SparkSession.builder().getOrCreate().sparkContext.applicationId

    var timer = System.currentTimeMillis()
    leftRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)
    leftRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
    val left = leftRDD.indexedRDD.rdd
    left.count
    val right = rightRDD.spatialPartitionedRDD.rdd
    right.count
    val distance = right.take(1).head.getRadius
    logger.info(s"TIMER|Index|Indexing|$appId|${getTime(timer)}")

    //
    timer = System.currentTimeMillis()
    val pairs = left.zipPartitions(right, preservesPartitioning = true){ (indexIt, circlesIt) =>
      if(!indexIt.hasNext || !circlesIt.hasNext){
        List.empty[(Point, Point)].toIterator
      } else {
        val index: SpatialIndex = indexIt.next()
        val B = circlesIt.toVector

        //
        for(b <- B){
          val nA = index.query(b.getEnvelopeInternal).size
          val global_gid = TaskContext.getPartitionId
          val local_gid = b.getUserData.toString().split("\t")(0)
          val nB = 1
          val ops = nA * nB
          logger.info(s"DEBUG|Index|$global_gid|$local_gid|$nA|$nB|$ops|$appId")
        }

        val pairs = for{
          b <- B
          a <- index.query(b.getEnvelopeInternal).asScala.map(_.asInstanceOf[Point])
          if { isWithin(a, b) }
        } yield{
          (circle2point(b), a)
        }
        pairs.toIterator
      }
    }
    logger.info(s"TIMER|Index|Joining|$appId|${getTime(timer)}")
    pairs
  }

  def partitionBased(leftRDD: SpatialRDD[Point],
    rightRDD: CircleRDD,
    threshold: Int = 1000,
    lparts: Int = 0,
    capacity: Int = 10,
    fraction: Double = 0.025,
    levels: Int = 6,
    buildOnSpatialPartitionedRDD: Boolean = true)
    (implicit global_grids: Vector[Envelope]): RDD[ (Point, Point)] = {

    val left = leftRDD.spatialPartitionedRDD.rdd
    val right = rightRDD.spatialPartitionedRDD.rdd

    left.zipPartitions(right, preservesPartitioning = true){ (leftIt, rightIt) =>
      if(!leftIt.hasNext || !rightIt.hasNext){
        List.empty[(Point, Point)].toIterator
      } else {
        // Building the local quadtree...
        val global_gid = TaskContext.getPartitionId
        val gridEnvelope = global_grids(global_gid)
        val grid = new QuadRectangle(gridEnvelope)
        val A = leftIt.toVector
        val B = rightIt.toVector
        val p = A.size * B.size

        if(p < threshold){
          // If there are not enoguh points, let's use the baseline strategy...
          val pairs = for{
            a <- A
            b <- B if isWithin(a, b)
          } yield{
            (circle2point(b), a)
          }
          pairs.toIterator
        } else {
          val quadtree = if(lparts == 0){ // Create a quadtree by capacity, fraction and levels...
            logger.info("DEBUG|By Capacity")
            val data = A.union(B)
            val n = data.length
            val fraction = 1 - computeFraction(n.toDouble)
            val sampleSize = fraction * n
            val sample = Random.shuffle(data).take(sampleSize.toInt)
            val quadtree = new StandardQuadTree[Int](grid, 0, capacity, levels)
            sample.foreach { p =>
              quadtree.insert(new QuadRectangle(p.getEnvelopeInternal), 1)
            }
            quadtree.assignPartitionIds()
            quadtree
          } else { // Create a quadtre by number of partitions...
            logger.info("DEBUG|By N Partitions")
            val data = A.union(B)
            val n = data.length
            val fraction = 1 - computeFraction(n.toDouble)
            val sampleSize = fraction * n
            val sample = Random.shuffle(data).take(sampleSize.toInt)
            val capacity = if(sample.size <= 2 * lparts){
              lparts
            } else {
              sample.size / lparts
            }
            val quadtree = new StandardQuadTree[Int](grid, 0, capacity, levels)
            sample.foreach { p =>
              quadtree.insert(new QuadRectangle(p.getEnvelopeInternal), 1)
            }
            quadtree.assignPartitionIds()
            quadtree
          }

          val L = A.flatMap{ a =>
            val r = new QuadRectangle(a.getEnvelopeInternal)
            quadtree.findZones(r).asScala.map(z => (a, z.partitionId))
          }

          val R = B.flatMap{ b =>
            val r = new QuadRectangle(b.getEnvelopeInternal)
            quadtree.findZones(r).asScala.map(z => (b, z.partitionId))
          }

          val pairs = for{
              a <- L
              b <- R if a._2 == b._2 & isWithin(a._1, b._1)
            } yield {
              (circle2point(b._1), a._1)
            }
          pairs.toIterator
        }
      }
    }
  }

  def partitionBasedDebug(leftRDD: SpatialRDD[Point],
    rightRDD: CircleRDD,
    threshold: Int = 1000,
    lparts: Int = 0,
    capacity: Int = 10,
    fraction: Double = 0.025,
    levels: Int = 6,
    buildOnSpatialPartitionedRDD: Boolean = true)
    (implicit global_grids: Vector[Envelope]): RDD[ (Point, Point)] = {

    //
    val appId = SparkSession.builder().getOrCreate().sparkContext.applicationId
    var timer = System.currentTimeMillis()
    val left = leftRDD.spatialPartitionedRDD.rdd
    val right = rightRDD.spatialPartitionedRDD.rdd
    logger.info(s"TIMER|Partition|Getting points and centers|$appId|-1|${getTime(timer)}")

    left.zipPartitions(right, preservesPartitioning = true){ (leftIt, rightIt) =>
      if(!leftIt.hasNext || !rightIt.hasNext){
        List.empty[(Point, Point)].toIterator
      } else {
        // Building the local quadtree...

        // Setting local variables...
        val global_gid = TaskContext.getPartitionId
        val gridEnvelope = global_grids(global_gid)
        val grid = new QuadRectangle(gridEnvelope)
        val A = leftIt.toVector
        val B = rightIt.toVector
        val p = A.size * B.size

        if(p < threshold){
          // If there are not enoguh points, let's use the baseline strategy...
          val timer = System.currentTimeMillis()
          val pairs = for{
            a <- A
            b <- B if isWithin(a, b)
          } yield{
            (circle2point(b), a)
          }
          logger.info(s"OPS|Partition|Join by baseline|$global_gid|0|${A.size}|${B.size}|$appId")
          logger.info(s"TIMER|Partition|Join by baseline|$appId|$global_gid|${getTime(timer)}")
          
          pairs.toIterator
        } else {
          var timer = System.currentTimeMillis
          val quadtree = if(lparts == 0){ // Create a quadtree by capacity, fraction and levels...
            val data = A.union(B)
            val n = data.length
            val sampleSize = fraction * n
            val sample = Random.shuffle(data).take(sampleSize.toInt)
            val quadtree = new StandardQuadTree[Int](grid, 0, capacity, levels)
            sample.foreach { p =>
              quadtree.insert(new QuadRectangle(p.getEnvelopeInternal), 1)
            }
            quadtree.assignPartitionIds()
            logger.info(f"QUADTREE|By C|$appId|$global_gid|$capacity|$fraction%1.2f|$levels|${A.size}|${B.size}|$n|${sample.size}|${quadtree.getLeafZones.size}")
            quadtree
          } else { // Create a quadtre by number of partitions...
            val data = A.union(B)
            val n = data.length
            val fraction = 1 - computeFraction(n.toDouble)
            val sampleSize = fraction * n
            val sample = Random.shuffle(data).take(sampleSize.toInt)
            val capacity = if(sample.size <= 2 * lparts){
              lparts
            } else {
              sample.size / lparts
            }
            val quadtree = new StandardQuadTree[Int](grid, 0, capacity, levels)
            sample.foreach { p =>
              quadtree.insert(new QuadRectangle(p.getEnvelopeInternal), 1)
            }
            quadtree.assignPartitionIds()
            logger.info(f"QUADTREE|By P|$appId|$global_gid|$capacity|$fraction%1.2f|$levels|$n|${sample.size}|${quadtree.getLeafZones.size}")
            quadtree
          }
          logger.info(s"TIMER|Partition|Getting quadtree|$appId|$global_gid|${getTime(timer)}")

          //
          save{s"/tmp/edgesLGrids_${global_gid}.wkt"}{
            quadtree.getLeafZones.asScala.map{ l =>
              s"${envelope2polygon(l.getEnvelope).toText()}\t${global_gid}\t${l.partitionId}\n"
            }
          }

          timer = System.currentTimeMillis
          val L = A.flatMap{ a =>
            val r = new QuadRectangle(a.getEnvelopeInternal)
            quadtree.findZones(r).asScala.map(z => (a, z.partitionId))
          }
          logger.info(s"TIMER|Partition|Feeding points|$appId|$global_gid|${getTime(timer)}")

          timer = System.currentTimeMillis
          val R = B.flatMap{ b =>
            val r = new QuadRectangle(b.getEnvelopeInternal)
            quadtree.findZones(r).asScala.map(z => (b, z.partitionId))
          }
          logger.info(s"TIMER|Partition|Feeding centers|$appId|$global_gid|${getTime(timer)}")

          timer = System.currentTimeMillis
          val pairs = for{
              a <- L
              b <- R if a._2 == b._2 & isWithin(a._1, b._1)
            } yield {
              (circle2point(b._1), a._1)
            }
          logger.info(s"TIMER|Partition|Join by partitions|$appId|${getTime(timer)}")
          val data = L.groupBy(_._2).map(l => (l._1.toInt, ("A", l._2.size))).toList ++
          R.groupBy(_._2).map(r => (r._1.toInt, ("B", r._2.size))).toList
          data.groupBy(_._1).foreach{ case(k, v) =>
            val arr = v.map(_._2)
            val value = arr.size match {
              case 2 => arr.sortBy(_._1).map(_._2).mkString("|")
              case 1 => {
                val h = arr.head
                if(h._1 == "A"){
                  s"${h._2}|0"
                } else {
                  s"0|${h._2}"
                }
              }
            }
            logger.info(s"OPS|Partition|Join by partitions|$global_gid|$k|$value|$appId")
          }
          pairs.toIterator
        }
      }
    }
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

  def groupByLeftPoint(pairs: RDD[(Point, Point)]): RDD[(Point, Set[Point])] = {
    pairs.map{ case(l, r) =>
      (l, Set(r))
    }.reduceByKey {_ ++ _}
  }

  def groupByRightPoint(pairs: RDD[(Point, Point)]): RDD[(Point, Set[Point])] = {
    pairs.map{ case(l, r) =>
      (r, Set(l))
    }.reduceByKey {_ ++ _}
  }

  def circle2point(circle: org.datasyslab.geospark.geometryObjects.Circle): Point = {
    val point = geofactory.createPoint(circle.getCenterPoint)
    point.setUserData(circle.getUserData)
    point
  }

  def isWithin(a: Point, b: Circle, distance: Double): Boolean = {
    val x = a.getX - b.getCenterPoint.x
    val y = a.getY - b.getCenterPoint.y
    val x2 = x * x
    val y2 = y * y
    math.sqrt(x2 + y2) <= distance
  }

  def isWithin(a: Point, b: Circle): Boolean = {
    val x = a.getX - b.getCenterPoint.x
    val y = a.getY - b.getCenterPoint.y
    val x2 = x * x
    val y2 = y * y
    math.sqrt(x2 + y2) <= b.getRadius
  }

  def computeFraction(x: Double, min: Double = 1000, max: Double = 30000,
    a: Double = 0, b: Double = 0.9): Double = {
    val v = x match {
      case x if x < min => min
      case x if x > max => max
      case _ => x
    }
    val ba  = b - a
    val num = v - min
    val den = max - min

    a + ((ba * num) / den)
  }
}

