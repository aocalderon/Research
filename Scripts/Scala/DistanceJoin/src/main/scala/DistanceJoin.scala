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

case class LocalGrid(envelope: Envelope, id: Int)

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
    threshold: Int = 100000,
    lgrids: Int = 4,
    capacity: Int = 10,
    fraction: Double = 0.025,
    levels: Int = 8,
    buildOnSpatialPartitionedRDD: Boolean = true)
    (implicit global_grids: Vector[Envelope]): RDD[ (Point, Point)] = {

    leftRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)
    leftRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
    rightRDD.spatialPartitioning(leftRDD.getPartitioner)
    rightRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)
    rightRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)

    val left = leftRDD.indexedRDD.rdd
    val right = rightRDD.indexedRDD.rdd

    left.zipPartitions(right, preservesPartitioning = true){ (leftIt, rightIt) =>
      if(!leftIt.hasNext || !rightIt.hasNext){
        List.empty[(Point, Point)].toIterator
      } else {
        // Building the local quadtree...
        val leftIndex = leftIt.next()
        val rightIndex = rightIt.next()
        val global_gid = TaskContext.getPartitionId
        val gridEnvelope = global_grids(global_gid)
        val grid = new QuadRectangle(gridEnvelope)
        val A = leftIndex.query(gridEnvelope).asScala.map(_.asInstanceOf[Point])
        val B = rightIndex.query(gridEnvelope).asScala.map(_.asInstanceOf[Circle])
        val n = A.size + B.size

        if(n < threshold){
          // If there are not enoguh points, let's use the index-base strategy...
          val pairs = for{
            b <- B
            a <- leftIndex.query(b.getEnvelopeInternal).asScala.map(_.asInstanceOf[Point])
                 if isWithin(a, b)
          } yield{
            (circle2point(b), a)
          }
          pairs.toIterator
        } else {
          // If there are enough points, let's use the partition-based strategy...
          /*
          val data = A.union(B)
          val sampleSize = (0.01 * data.length).toInt
          val sample = Random.shuffle(data).take(sampleSize)
          val quadtree = new StandardQuadTree[Int](grid, 0, data.length / 4, levels)
          sample.foreach { p =>
            quadtree.insert(new QuadRectangle(p.getEnvelopeInternal), 1)
          }
          quadtree.assignPartitionIds()
          val local_grids = quadtree.getLeafZones.asScala
           */

          val local_grids = getLocalGrids(gridEnvelope, lgrids)

          val pairs = local_grids.flatMap{ local_grid =>
            val A = leftIndex.query(local_grid.envelope).asScala.map(_.asInstanceOf[Point])
            val B = rightIndex.query(local_grid.envelope).asScala.map(_.asInstanceOf[Circle]).toSet

            for{
              a <- A
              b <- B if isWithin(a, b)
            } yield {
              (circle2point(b), a)
            }
          }
          pairs.toIterator
        }
      }
    }
  }

  def partitionBasedDebug(leftRDD: SpatialRDD[Point],
    rightRDD: CircleRDD,
    threshold: Int = 5000,
    lparts: Int = 0,
    capacity: Int = 10,
    fraction: Double = 0.025,
    levels: Int = 5,
    buildOnSpatialPartitionedRDD: Boolean = true)
    (implicit global_grids: Vector[Envelope]): RDD[ (Point, Point)] = {

    //
    val appId = SparkSession.builder().getOrCreate().sparkContext.applicationId

    var timer = System.currentTimeMillis()
    leftRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)
    leftRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
    val left = leftRDD.indexedRDD.rdd
    left.count
    logger.info(s"TIMER|Partition|Indexing Centers|$appId|${getTime(timer)}")

    timer = System.currentTimeMillis()
    rightRDD.spatialPartitioning(leftRDD.getPartitioner)
    rightRDD.buildIndex(IndexType.QUADTREE, buildOnSpatialPartitionedRDD)
    rightRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)
    val right = rightRDD.indexedRDD.rdd
    right.count
    logger.info(s"TIMER|Partition|Indexing Points|$appId|${getTime(timer)}")

    left.zipPartitions(right, preservesPartitioning = true){ (leftIt, rightIt) =>
      if(!leftIt.hasNext || !rightIt.hasNext){
        List.empty[(Point, Point)].toIterator
      } else {
        // Building the local quadtree...
        timer = System.currentTimeMillis()
        val leftIndex = leftIt.next()
        val rightIndex = rightIt.next()
        val global_gid = TaskContext.getPartitionId
        val gridEnvelope = global_grids(global_gid)
        val grid = new QuadRectangle(gridEnvelope)
        val A = leftIndex.query(gridEnvelope).asScala.map(_.asInstanceOf[Point])
        val B = rightIndex.query(gridEnvelope).asScala.map(_.asInstanceOf[Circle])
        val n = A.size * B.size
        logger.info(s"TIMER|Partition|Setting variables|$appId|${getTime(timer)}")

        logger.info(s"THRESHOLD|Partition|$global_gid|$n|$threshold|$appId")
        if(n < threshold){
          // If there are not enoguh points, let's use the index-base strategy...
          timer = System.currentTimeMillis()

          //
          for(b <- B){
            val nA = leftIndex.query(b.getEnvelopeInternal).size
            val global_gid = TaskContext.getPartitionId
            val local_gid = b.getUserData.toString().split("\t")(0)
            val nB = 1
            val ops = nA * nB
            logger.info(s"DEBUG|PartitionI|$global_gid|$local_gid|$nA|$nB|$ops|$appId")
          }

          val pairs = for{
            b <- B
            a <- leftIndex.query(b.getEnvelopeInternal).asScala.map(_.asInstanceOf[Point])
            if { isWithin(a, b) }
          } yield{
            (circle2point(b), a)
          }
          logger.info(s"TIMER|Partition|Joining by index|$appId|${getTime(timer)}")
          pairs.toIterator
        } else {
          // If there are enough points, let's use the partition-based strategy...
          timer = System.currentTimeMillis()

          val local_grids = if(lparts == 0){
            logger.info(s"QUADTREE|Partition|$global_gid|$capacity|$fraction|$levels|$appId")
            val data = A.union(B)
            val sampleSize = (fraction * data.length).toInt
            val sample = Random.shuffle(data).take(sampleSize)
            val quadtree = new StandardQuadTree[Int](grid, 0, capacity, levels)
            sample.foreach { p =>
              quadtree.insert(new QuadRectangle(p.getEnvelopeInternal), 1)
            }
            quadtree.assignPartitionIds()
            val local_grids = quadtree.getLeafZones.asScala.map{ leaf =>
              LocalGrid(leaf.getEnvelope, leaf.partitionId)
            }

            local_grids
          } else {
            logger.info(s"QUADTREE|Partition|$global_gid|$lparts|$appId")
            val data = A.union(B)
            val n = data.length
            val levels = lparts
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
            val local_grids = quadtree.getLeafZones.asScala.map{ leaf =>
              LocalGrid(leaf.getEnvelope, leaf.partitionId)
            }

            local_grids
          }

          //
          val ggid = TaskContext.getPartitionId
          save{s"/tmp/edgesLGrids_${ggid}.wkt"}{
            local_grids.map{ l =>
              s"${envelope2polygon(l.envelope).toText()}\t${ggid}\t${l.id}\n"
            }
          }

          val pairs = local_grids.flatMap{ local_grid =>
            val A = leftIndex.query(local_grid.envelope).asScala.map(_.asInstanceOf[Point])
            val B = rightIndex.query(local_grid.envelope).asScala.map(_.asInstanceOf[Circle]).toSet

            //
            val global_gid = TaskContext.getPartitionId
            val local_gid = local_grid.id
            val nA = A.size
            val nB = B.size
            val ops = nA * nB
            logger.info(s"DEBUG|PartitionP|$global_gid|$local_gid|$nA|$nB|$ops|$appId")

            for{
              a <- A
              b <- B if isWithin(a, b)
            } yield {
              (circle2point(b), a)
            }
          }
          logger.info(s"TIMER|Partition|Joining by partition|$appId|${getTime(timer)}")
          pairs.toIterator
        }
      }
    }
  }

  def getLocalGrids(boundary: Envelope, n: Int = 4): Seq[LocalGrid] = {
    val intervalX = (boundary.getMaxX() - boundary.getMinX()) / n;
    val intervalY = (boundary.getMaxY() - boundary.getMinY()) / n;

    val lgrids = for{
      i <- 0 until  n
      j <- 0 until  n
    } yield {
      new Envelope(
        boundary.getMinX() + intervalX * i,
        boundary.getMinX() + intervalX * (i + 1),
        boundary.getMinY() + intervalY * j,
        boundary.getMinY() + intervalY * (j + 1)
      )
    }

    lgrids.zipWithIndex.map(l => LocalGrid(l._1, l._2))
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

  def computeFraction(x: Double, min: Double = 100, max: Double = 10000,
    a: Double = 0, b: Double = 0.99): Double = {
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

