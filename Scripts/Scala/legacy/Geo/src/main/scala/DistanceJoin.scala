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
import org.datasyslab.geospark.geometryObjects.Circle
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate, Point, Polygon, MultiPolygon}
import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.index.SpatialIndex
import com.vividsolutions.jts.index.quadtree._
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

  def getTime(time: Long): Long = { (System.currentTimeMillis() - time ) }

  def partitionBasedQuadtree(leftRDD: PointRDD,
    rightRDD: CircleRDD,
    distance: Double,
    capacity: Int = 200,
    fraction: Double = 0.1,
    levels: Int = 5)
    (implicit grids: Vector[Envelope]): RDD[ (Vector[ (Point, Vector[Point]) ], String) ] = {

    val A = leftRDD.spatialPartitionedRDD.rdd
    val B = rightRDD.spatialPartitionedRDD.rdd
    A.zipPartitions(B, preservesPartitioning = true){ (pointsIt, circlesIt) =>
      var results = scala.collection.mutable.ListBuffer[ (Vector[ (Point, Vector[Point]) ], String) ]()
      if(!pointsIt.hasNext || !circlesIt.hasNext){
        val pairs = Vector.empty[ (Point, Vector[Point]) ]
        val partition_id = TaskContext.getPartitionId
        val gridEnvelope = grids(partition_id)
        val stats = f"${envelope2polygon(gridEnvelope).toText()}\t" +
        f"${partition_id}\t${0}\t${0}\t" +
        f"${0}\t${0}\t${0}\t${0}\n"
        results += ((pairs, stats))
        results.toIterator
      } else {
        // Getting the global grid...
        var timer = System.currentTimeMillis()
        val partition_id = TaskContext.getPartitionId
        val gridEnvelope = grids(partition_id)
        val grid = new QuadRectangle(gridEnvelope)

        // Building the local quadtree...
        val pointsA = pointsIt.toVector
        val pointsB = circlesIt.toVector
        val quadtree = new StandardQuadTree[Geometry](grid, 0, capacity, levels)
        val sampleSize = (fraction * pointsB.length).toInt
        val sample = Random.shuffle(pointsB).take(sampleSize)
        sample.foreach { p =>          
          quadtree.insert(new QuadRectangle(p.getEnvelopeInternal), p)
        }
        quadtree.assignPartitionIds()
        val timer1 = getTime(timer)

        // Locating pointsA...
        timer = System.currentTimeMillis()
        val keyPointsA = pointsA.flatMap{ point =>
          val query = new QuadRectangle(point.getEnvelopeInternal) 
          val zones = quadtree.findZones(query)
          zones.asScala.map{ zone => (zone.partitionId.toInt, point) }
        }
        val timer2 = getTime(timer)

        // Locating pointsB...
        timer = System.currentTimeMillis()
        val keyPointsB = pointsB.flatMap{ point =>
          val query = new QuadRectangle(point.getEnvelopeInternal) 
          val zones = quadtree.findZones(query)
          val center = geofactory.createPoint(point.getCenterPoint)
          center.setUserData(point.getUserData)
          zones.asScala.map{ zone => (zone.partitionId.toInt, center) }
        }
        val timer3 = getTime(timer)

        // Finding pairs
        timer = System.currentTimeMillis()
        val candidates = for{
          a <- keyPointsA
          b <- keyPointsB if a._1 == b._1 
        } yield {
          (a, b, a._2.distance(b._2))
        }

        val pairs = candidates.filter(_._3 <= distance).map{ case(a, b, d) => (a._2, b._2)}
          .groupBy(_._1).toVector.map{ case(p, pairs) => (p, pairs.map(_._2))}
        val timer4 = getTime(timer)

        // Report results...
        val stats = f"${envelope2polygon(gridEnvelope).toText()}\t" +
        f"${partition_id}\t${pointsA.size}\t${pointsB.size}\t" +
        f"${timer1}\t${timer2}\t${timer3}\t${timer4}\n"
        results += ((pairs, stats))
        results.toIterator
      }
    }
  }  

  def partitionBasedQuadtreeV1(leftRDD: PointRDD,
    rightRDD: CircleRDD,
    distance: Double,
    capacity: Int = 200,
    fraction: Double = 0.1,
    levels: Int = 5)
    (implicit grids: Vector[Envelope]): RDD[ (Vector[ (Point, Vector[Point]) ], String, String, String) ] = {

    val A = leftRDD.spatialPartitionedRDD.rdd
    val B = rightRDD.spatialPartitionedRDD.rdd
    A.zipPartitions(B, preservesPartitioning = true){ (pointsIt, circlesIt) =>
      var results = scala.collection.mutable.ListBuffer[ (Vector[ (Point, Vector[Point]) ], String, String, String) ]()
      if(!pointsIt.hasNext || !circlesIt.hasNext){
        val pairs = Vector.empty[ (Point, Vector[Point]) ]
        val global_pid = TaskContext.getPartitionId
        val gridEnvelope = grids(global_pid)
        val stats = f"${envelope2polygon(gridEnvelope).toText()}\t" +
        f"${global_pid}\t${0}\t${0}\t" +
        f"${0}\t${0}\t${0}\t${0}\n"
        results += ((pairs, stats, "", ""))
        results.toIterator
      } else {
        // Building the local quadtree...
        var timer = System.currentTimeMillis()
        val global_pid = TaskContext.getPartitionId
        val gridEnvelope = grids(global_pid)
        gridEnvelope.expandBy(distance)
        val grid = new QuadRectangle(gridEnvelope)
        
        val pointsA = pointsIt.toVector.map{ a =>
          a.setUserData("A")
          a
        }
        val pointsB = circlesIt.toVector.map{ b =>
          val point = geofactory.createPoint(b.getCenterPoint)
          point.setUserData(b.getUserData.toString)
          point
        }
        val nPointsA = pointsA.size
        val nPointsB = pointsB.size
        val points =  pointsA ++ pointsB
        val nPoints = points.size
        val quad = new StandardQuadTree[Point](grid, 0, capacity, 10)
        val timer1 = getTime(timer)

        // Feeding the quadtree A...
        timer = System.currentTimeMillis()
        pointsA.foreach { p =>
          quad.insert(new QuadRectangle(p.getEnvelopeInternal), p)
        }
        val timer2 = getTime(timer)

        // Feeding the quadtree B...
        timer = System.currentTimeMillis()
        pointsB.foreach { p =>
          quad.insert(new QuadRectangle(p.getEnvelopeInternal), p)
        }
        val timer3 = getTime(timer)

        // Report results...
        timer = System.currentTimeMillis()
        quad.assignPartitionIds
        val candidates = quad.getLeafZones.asScala.flatMap{ leaf =>
          val local_pid = leaf.partitionId
          val p = quad.getPointsByEnvelope(leaf.getEnvelope).size
          val query = leaf.getEnvelope
          query.expandBy(distance)
          val points = quad.getPointsByEnvelope(query).asScala.map{ p =>
            (p, p.getUserData.toString() == "A")
          }
          val A = points.filter(_._2).map(_._1).distinct
          val B = points.filterNot(_._2).map(_._1).filter(b =>
            leaf.getEnvelope.contains(b.getEnvelopeInternal)
          ).distinct

          val pairs = for{
            a <- A
            b <- B if a.distance(b) <= distance
          } yield {
            (a, b)
          }

          //
          //logger.info(s"$global_pid\t$local_pid\t$p\t${points.size}\t${A.size}\t${B.size}\t${A.size * B.size}\t${pairs.size}")

          pairs
        }
        val pairs = candidates.groupBy(_._1).toVector.map{ case(p, pairs) => (p, pairs.map(_._2).toVector)}
        val timer4 = getTime(timer)
        
        val stats = f"${envelope2polygon(gridEnvelope).toText()}\t" +
        f"${global_pid}\t${nPointsA}\t${nPointsB}\t" +
        f"${timer1}\t${timer2}\t${timer3}\t${timer4}\n"
        val lgridsWKT = quad.getLeafZones.asScala.map{ leaf =>
          s"${envelope2polygon(leaf.getEnvelope).toText()}\t${leaf.partitionId}\n"
        }.mkString("")
        val pointsWKT = points.map{ point =>
          s"${envelope2polygon(point.getEnvelopeInternal).getCentroid.toText()}\t${point.getUserData.toString == "A"}\t$global_pid\n"
        }.mkString("")
        results += ((pairs, stats, lgridsWKT, pointsWKT))
        results.toIterator
      }
    }
  }  

  def partitionBasedQuadtreeV2(leftRDD: PointRDD,
    rightRDD: CircleRDD,
    distance: Double,
    capacity: Int = 200,
    fraction: Double = 0.1,
    levels: Int = 5)
    (implicit grids: Vector[Envelope]): RDD[ (Vector[ (Point, Vector[Point]) ], String, String, String) ] = {

    val A = leftRDD.spatialPartitionedRDD.rdd
    val B = rightRDD.spatialPartitionedRDD.rdd
    A.zipPartitions(B, preservesPartitioning = true){ (pointsIt, circlesIt) =>
      var results = scala.collection.mutable.ListBuffer[ (Vector[ (Point, Vector[Point]) ], String, String, String) ]()
      if(!pointsIt.hasNext || !circlesIt.hasNext){
        val pairs = Vector.empty[ (Point, Vector[Point]) ]
        val global_pid = TaskContext.getPartitionId
        val gridEnvelope = grids(global_pid)
        val stats = f"${envelope2polygon(gridEnvelope).toText()}\t" +
        f"${global_pid}\t${0}\t${0}\t" +
        f"${0}\t${0}\t${0}\t${0}\n"
        results += ((pairs, stats, "", ""))
        results.toIterator
      } else {
        // Building the local quadtree...
        var timer = System.currentTimeMillis()
        val global_pid = TaskContext.getPartitionId
        val gridEnvelope = grids(global_pid)
        //gridEnvelope.expandBy(distance)
        val grid = new QuadRectangle(gridEnvelope)

        val pointsA = pointsIt.toVector
        val pointsB = circlesIt.toVector
        val nPointsA = pointsA.size
        val nPointsB = pointsB.size

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
          val zones = quadtree.findZones(query)
          zones.asScala.map{ zone => (zone.partitionId.toInt, point, zone.getEnvelope) }
        }
        val timer2 = getTime(timer)

        // Feeding B...
        timer = System.currentTimeMillis()
        val B = pointsB.flatMap{ point =>
          val query = new QuadRectangle(point.getEnvelopeInternal) 
          val zones = quadtree.findZones(query)
          val b = geofactory.createPoint(point.getCenterPoint)
          b.setUserData(point.getUserData)
          zones.asScala.map{ zone => (zone.partitionId.toInt, b) }
        }
        val timer3 = getTime(timer)

        // Find pairs
        timer = System.currentTimeMillis()
        val candidates = for{
          a <- A
          b <- B if a._1 == b._1 && a._2.distance(b._2) <= distance 
        } yield {
          (a._2, b._2, a._3.contains(a._2.getEnvelopeInternal))
        }
        val pairs = candidates.filter(_._3).groupBy(_._1).toVector.map{ case(p, pairs) => (p, pairs.map(_._2))}
        val timer4 = getTime(timer)

        // Report results...
        val stats = f"${envelope2polygon(gridEnvelope).toText()}\t" +
        f"${global_pid}\t${nPointsA}\t${nPointsB}\t" +
        f"${timer1}\t${timer2}\t${timer3}\t${timer4}\n"
        val lgridsWKT = quadtree.getLeafZones.asScala.map{ leaf =>
          s"${envelope2polygon(leaf.getEnvelope).toText()}\t${leaf.partitionId}\n"
        }.mkString("")
        val pointsWKT = (pointsA ++ pointsB).map{ point =>
          s"${envelope2polygon(point.getEnvelopeInternal).getCentroid.toText()}\t${point.getUserData.toString == "A"}\t$global_pid\n"
        }.mkString("")
        results += ((pairs, stats, lgridsWKT, pointsWKT))
        results.toIterator
      }
    }
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
