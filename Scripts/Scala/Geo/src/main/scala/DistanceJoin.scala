import org.slf4j.{LoggerFactory, Logger}
import org.rogach.scallop._
import scala.collection.JavaConverters._
import scala.util.Random
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
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate, Point, Polygon, MultiPolygon}
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.index.SpatialIndex;
import edu.ucr.dblab.Utils._
import edu.ucr.dblab.{StandardQuadTree, QuadRectangle}

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

  def partitionBasedQuadtree(leftRDD: PointRDD, rightRDD: CircleRDD, distance: Double, capacity: Int = 200, fraction: Double = 0.1, levels: Int = 5)(implicit grids: Vector[Envelope]): RDD[(Point, Vector[Point])] = {

    val A = leftRDD.spatialPartitionedRDD.rdd
    val B = rightRDD.spatialPartitionedRDD.rdd
    A.zipPartitions(B, preservesPartitioning = true){ (pointsIt, circlesIt) =>
      if(!pointsIt.hasNext || !circlesIt.hasNext){
        List.empty[(Point, Vector[Point])].toIterator
      } else {
        // Getting the global grid...
        val partition_id = TaskContext.getPartitionId
        val gridEnvelope = grids(partition_id)
        val grid = new QuadRectangle(gridEnvelope)

        // Building the local quadtree...
        val pointsA = pointsIt.toVector
        val pointsB = circlesIt.toVector
        val quadtree = new StandardQuadTree[Int](grid, 0, capacity, levels)
        val sampleSize = (fraction * pointsB.length).toInt
        val sample = Random.shuffle(pointsB).take(sampleSize)
        sample.foreach { p =>
          quadtree.insert(new QuadRectangle(p.getEnvelopeInternal), 1)
        }
        quadtree.assignPartitionIds()

        // Locating pointsA...
        val keyPointsA = pointsA.flatMap{ point =>
          val query = new QuadRectangle(point.getEnvelopeInternal) 
          val zones = quadtree.findZones(query)
          zones.asScala.map{ zone => (zone.partitionId.toInt, point) }
        }

        // Locating pointsB...
        val keyPointsB = pointsB.flatMap{ point =>
          val query = new QuadRectangle(point.getEnvelopeInternal) 
          val zones = quadtree.findZones(query)
          val center = geofactory.createPoint(point.getCenterPoint)
          center.setUserData(point.getUserData)
          zones.asScala.map{ zone => (zone.partitionId.toInt, center) }
        }

        // Finding pairs
        val candidates = for{
          a <- keyPointsA
          b <- keyPointsB if a._1 == b._1 
        } yield {
          (a, b, a._2.distance(b._2))
        }

        val pairs = candidates.filter(_._3 <= distance).map{ case(a, b, d) => (a._2, b._2)}
          .groupBy(_._1).toVector.map{ case(p, pairs) => (p, pairs.map(_._2))}

        // Report results...
        pairs.toIterator
      }
    }
  }  

  def partitionBasedQuadtree2(leftRDD: PointRDD, rightRDD: CircleRDD, distance: Double, capacity: Int = 20, fraction: Double = 0.1, levels: Int = 5)(implicit grids: Vector[Envelope]): RDD[(Vector[(Point, Vector[Point])], Vector[String])] = {

    val A = leftRDD.spatialPartitionedRDD.rdd
    val B = rightRDD.spatialPartitionedRDD.rdd
    A.zipPartitions(B, preservesPartitioning = true){ (pointsIt, circlesIt) =>
      var results = new scala.collection.mutable.ListBuffer[(Vector[(Point, Vector[Point])], Vector[String])]()

      if(!pointsIt.hasNext || !circlesIt.hasNext){
        results.toIterator
      } else {
        // Getting the global grid...
        val partition_id = TaskContext.getPartitionId
        val gridEnvelope = grids(partition_id)
        val grid = new QuadRectangle(gridEnvelope)

        // Building the local quadtree...
        val pointsA = pointsIt.toVector
        val pointsB = circlesIt.toVector

        val quadtree = new StandardQuadTree[(Point, Boolean)](grid, 0, capacity, levels)
        pointsA.foreach { p =>
          quadtree.insert(new QuadRectangle(p.getEnvelopeInternal), (p, true))
        }
        pointsB.foreach { p =>
          val center = geofactory.createPoint(p.getCenterPoint)
          center.setUserData(p.getUserData)
          quadtree.insert(new QuadRectangle(p.getEnvelopeInternal), (center, false))
        }
        quadtree.assignPartitionIds()

        val lgrids = quadtree.getLeafZones.asScala.map{ r =>
          val envelope = envelope2polygon(r.getEnvelope)
          val id = r.partitionId

          s"${envelope.toText()}\t${id}\n"
        }.toVector

        val pairs = quadtree.getElementsByPartition().asScala.flatMap{ case(id, nodes) =>
          val elements = nodes.asScala.map{ node =>
            val (point, isA) = node.getElement()
            val id = node.getPartitionID

            (id, point, isA)
          }
          val A = elements.filter(_._3).map(_._2)
          val B = elements.filterNot(_._3).map(_._2)

          val candidates = for{
            a <- A
            b <- B if a.distance(b) <= distance
          } yield {
            (a, b)
          }

          candidates.groupBy(_._1).toVector.map{ case(p, pairs) => (p, pairs.map(_._2).toVector)}
        }.toVector

        results += ((pairs, lgrids))
        results.toIterator
      }
    }
  }  

  def partitionBasedQuadtreeViz(leftRDD: PointRDD, rightRDD: CircleRDD, distance: Double, capacity: Int = 200, fraction: Double = 0.1, levels: Int = 5)(implicit grids: Vector[Envelope]): RDD[(Vector[(Int, Point)], Vector[(Int, Point)], Vector[(Point, Vector[Point])], List[String])] = {

    val A = leftRDD.spatialPartitionedRDD.rdd
    val B = rightRDD.spatialPartitionedRDD.rdd
    val results = A.zipPartitions(B, preservesPartitioning = true){ (pointsIt, circlesIt) =>
      var results = new scala.collection.mutable.ListBuffer[(Vector[(Int, Point)], Vector[(Int, Point)], Vector[(Point, Vector[Point])], List[String])]()
      if(!pointsIt.hasNext || !circlesIt.hasNext){
        results.toIterator
      } else {
        val partition_id = TaskContext.getPartitionId
        val gridEnvelope = grids(partition_id)
        //gridEnvelope.expandBy(distance)
        val grid = new QuadRectangle(gridEnvelope)

        val pointsA = pointsIt.toVector
        val pointsB = circlesIt.toVector
        val quadtree = new StandardQuadTree[Int](grid, 0, capacity, levels)
        val sampleSize = (fraction * pointsB.length).toInt
        val sample = Random.shuffle(pointsB).take(sampleSize)
        sample.foreach { p =>
          quadtree.insert(new QuadRectangle(p.getEnvelopeInternal), 1)
        }
        quadtree.assignPartitionIds()

        //  Collect envelopes just for visualization purposes...
        val lgrids = quadtree.getLeafZones.asScala.map{ zone =>
          (zone.partitionId, zone.getEnvelope)
        }
        val slgrids = lgrids.map(g => s"${envelope2polygon(g._2).toText()}\t${g._1}\n").toList

        // Locate pointsA...
        val keyPointsA = pointsA.flatMap{ point =>
          val query = new QuadRectangle(point.getEnvelopeInternal) 
          val zones = quadtree.findZones(query)
          zones.asScala.map{ zone => (zone.partitionId.toInt, point) }
        }

        // Locate pointsB...
        val keyPointsB = pointsB.flatMap{ point =>
          val query = new QuadRectangle(point.getEnvelopeInternal) 
          val zones = quadtree.findZones(query)
          val center = geofactory.createPoint(point.getCenterPoint)
          center.setUserData(point.getUserData)
          zones.asScala.map{ zone => (zone.partitionId.toInt, center) }
        }

        // Find pairs
        val candidates = for{
          a <- keyPointsA
          b <- keyPointsB if a._1 == b._1 
        } yield {
          (a, b, a._2.distance(b._2))
        }

        val pairs = candidates.filter(_._3 <= distance).map{ case(a, b, d) => (a._2, b._2)}
          .groupBy(_._1).toVector.map{ case(p, pairs) => (p, pairs.map(_._2))}

        // Report results...
        results += ((keyPointsA, keyPointsB, pairs, slgrids))
        results.toIterator
      }
    }
    results
  }  

  def partitionBasedGrid(leftRDD: PointRDD, rightRDD: PointRDD, distance: Double, w: Double = 0.0)(implicit grids: Vector[Envelope]): RDD[(Point, List[Point])] = {
    val width = if(w == 0.0) distance else w
    val circlesRDD = new CircleRDD(rightRDD, distance)
    circlesRDD.analyze(rightRDD.boundary(), rightRDD.countWithoutDuplicates().toInt)
    circlesRDD.spatialPartitioning(leftRDD.getPartitioner)
    circlesRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

    val A = leftRDD.spatialPartitionedRDD.rdd
    val B = circlesRDD.spatialPartitionedRDD.rdd
    A.zipPartitions(B, preservesPartitioning = true){ (pointsIt, circlesIt) =>
      if(!pointsIt.hasNext || !circlesIt.hasNext){
        List.empty[(Point, List[Point])].toIterator
      } else {
        val partition_id = TaskContext.getPartitionId
        val grid = grids(partition_id)

        // We will require to expand the boundary to include circles?
        //grid.expandBy(distance)

        val minX = grid.getMinX
        val minY = grid.getMinY
        val dX = grid.getMaxX - minX
        val cols = math.ceil(dX / width).toInt
        val dY = grid.getMaxY - minY
        val rows = math.ceil(dY / width).toInt

        // Partition points according to width...
        val ptuples = pointsIt.toVector
          .map(p => (p.getX, p.getY, p))        
          .map{ case(x,y,p) => (x - minX, y - minY, p)} // just for testing...

        val points = ptuples.map{ case(x, y, point) =>
          val i = math.floor(x / width).toInt
          val j = math.floor(y / width).toInt
          val id = i + j * cols

          ((id, point))
        }.toList

        // Partition circles according to width distributing replicates if needed...
        val circles = circlesIt.toVector
          .flatMap{c =>
            val e = c.getEnvelopeInternal
            val point = geofactory.createPoint(c.getCenterPoint)
            point.setUserData(c.getUserData)

            val points = List(
              (e.getMinX, e.getMinY, point),
              (e.getMaxX, e.getMinY, point),
              (e.getMaxX, e.getMaxY, point),
              (e.getMinX, e.getMaxY, point)
            ).map{ case(x,y,c) => (x - minX, y - minY, c)} // just for testing...

            points.map{ case(x, y, point) =>
              val i = math.floor(x / width).toInt
              val j = math.floor(y / width).toInt
              val id = i + j * cols
              if(i >= cols || i < 0 || j >= rows || j < 0)
                None
              else
                Some((id, point))
            }.flatten.distinct
          }.toList

        val candidates = for{
          point <- points
          circle <- circles if point._1 == circle._1 
        } yield {
          (point, circle, point._2.distance(circle._2))
        }

        val pairs = candidates.filter(_._3 <= distance).map{ case(p, c, d) => (p._2, c._2)}
          .groupBy(_._1).toList.map{ case(p, pairs) => (p, pairs.map(_._2))}

        // Report results...
        pairs.toIterator
      }
    }
  }

  def partitionBasedViz(leftRDD: PointRDD, rightRDD: PointRDD, distance: Double, w: Double = 0.0)(implicit grids: Vector[Envelope]): RDD[(List[(Int, Point)], List[(Int, Point)], List[(Point, List[Point])], List[String])] = {
    val width = if(w == 0.0) distance else w
    val circlesRDD = new CircleRDD(rightRDD, distance)
    circlesRDD.analyze(rightRDD.boundary(), rightRDD.countWithoutDuplicates().toInt)
    circlesRDD.spatialPartitioning(leftRDD.getPartitioner)
    circlesRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

    val A = leftRDD.spatialPartitionedRDD.rdd
    val B = circlesRDD.spatialPartitionedRDD.rdd
    val results = A.zipPartitions(B, preservesPartitioning = true){ (pointsIt, circlesIt) =>
      var results = new scala.collection.mutable.ListBuffer[(List[(Int, Point)], List[(Int, Point)], List[(Point, List[Point])], List[String])]()
      if(!pointsIt.hasNext || !circlesIt.hasNext){
        results.toIterator
      } else {
        val partition_id = TaskContext.getPartitionId
        val grid = grids(partition_id)

        // We will require to expand the boundary to include circles?
        //grid.expandBy(distance)

        val minX = grid.getMinX
        val minY = grid.getMinY
        val dX = grid.getMaxX - minX
        val cols = math.ceil(dX / width).toInt
        val dY = grid.getMaxY - minY
        val rows = math.ceil(dY / width).toInt

        //  Collect envelopes just for visualization purposes...
        val lgrids = for{
          i <- 0 to cols - 1
          j <- 0 to rows - 1
        } yield { 
          val p = envelope2polygon(new Envelope(minX + width * i, minX + width * (i + 1), minY + width * j, minY + width * (j + 1)))
          p.setUserData(s"${i + j * cols}")
          p
        }
        import org.geotools.geometry.jts.GeometryClipper
        val clipper = new GeometryClipper(grid)

        val slgrids = lgrids.map{ g =>
          val grid = clipper.clip(g, true)
          s"${grid.toText()}\t${g.getUserData.toString}\t${partition_id}\n"
        }.toList

        // Partition points according to width...
        val ptuples = pointsIt.toVector
          .map(p => (p.getX, p.getY, p))        
          .map{ case(x,y,p) => (x - minX, y - minY, p)} // just for testing...

        val points = ptuples.map{ case(x, y, point) =>
          val i = math.floor(x / width).toInt
          val j = math.floor(y / width).toInt
          val id = i + j * cols

          ((id, point))
        }.toList

        // Partition circles according to width distributing replicates if needed...
        val circles = circlesIt.toVector
          .flatMap{c =>
            val e = c.getEnvelopeInternal
            val point = geofactory.createPoint(c.getCenterPoint)
            point.setUserData(c.getUserData)

            val points = List(
              (e.getMinX, e.getMinY, point),
              (e.getMaxX, e.getMinY, point),
              (e.getMaxX, e.getMaxY, point),
              (e.getMinX, e.getMaxY, point)
            ).map{ case(x,y,c) => (x - minX, y - minY, c)} // just for testing...

            points.map{ case(x, y, point) =>
              val i = math.floor(x / width).toInt
              val j = math.floor(y / width).toInt
              val id = i + j * cols
              if(i >= cols || i < 0 || j >= rows || j < 0)
                None
              else
                Some((id, point))
            }.flatten.distinct
          }.toList

        val candidates = for{
          point <- points
          circle <- circles if point._1 == circle._1 
        } yield {
          (point, circle, point._2.distance(circle._2))
        }

        val pairs = candidates.filter(_._3 <= distance).map{ case(p, c, d) => (p._2, c._2)}
          .groupBy(_._1).toList.map{ case(p, pairs) => (p, pairs.map(_._2))}

        // Report results...
        results += ((points, circles, pairs, slgrids))
        results.toIterator
      }
    }
    results
  }  
}
