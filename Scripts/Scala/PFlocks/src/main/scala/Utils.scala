package edu.ucr.dblab.pflock

import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel}
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate, Polygon, Point}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.serializer.KryoSerializer

import org.datasyslab.geospark.spatialRDD.{CircleRDD, SpatialRDD}
import org.datasyslab.geospark.geometryObjects.Circle
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.enums.GridType

import org.slf4j.{Logger, LoggerFactory}
//import org.jgrapht.graph.{SimpleGraph, DefaultEdge}
//import org.jgrapht.Graphs

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object Utils {

  //** Implicits

  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  //** Case classes

  case class Data(id: Int, t: Int)

  case class Cell(id: Int, lineage: String, envelope: Envelope)

  case class Settings(epsilon_prime: Double, mu: Int, tolerance: Double,
    debug: Boolean, appId: String, storageLevel: StorageLevel){
    val scale = 1 / tolerance
    val epsilon = epsilon_prime + tolerance
    val r = (epsilon_prime / 2.0) + tolerance
    val r2 = math.pow(epsilon_prime / 2.0, 2) + tolerance
  }

  //** Parallel functions

  def getDisks(points: SpatialRDD[Point], centers: CircleRDD)
    (implicit settings: Settings, logger: Logger):  RDD[Point] = {

    val mu = settings.mu
    centers.spatialPartitioning(points.getPartitioner)

    JoinQuery.DistanceJoinQuery(points, centers, true, false).rdd
      .filter(_._2.size >= mu) // Removing less than mu...
      .map{ case(center, points) =>
        val pids = points.asScala.map(_.getUserData.asInstanceOf[Data].id).toList.sorted
        center.setUserData(pids)

        center.asInstanceOf[Point]
      }
  }

  def computeCenters(pairs: RDD[(Point, Point)])
    (implicit geofactory: GeometryFactory, settings: Settings): CircleRDD = {

    val r2 = settings.r2
    val r  = settings.r
    val centers = pairs.mapPartitions{ pairsIt =>
      pairsIt.map{ case(p1, p2) =>
        calculateCenterCoordinates(p1, p2, r2)
      }.flatten
    }.map{ center =>
      new Circle(center, r)
    }

    new CircleRDD(centers)
  }

  def calculateCenterCoordinates(p1: Point, p2: Point, r2: Double)
    (implicit geofactory: GeometryFactory, settings: Settings): List[Point] = {

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
      val ids = List(p1.getUserData.asInstanceOf[Data], p2.getUserData.asInstanceOf[Data])
      h.setUserData(ids)
      k.setUserData(ids)
      List(h, k)
    } else {
      val p2_prime = geofactory.createPoint(new Coordinate(p2.getX + settings.tolerance,
        p2.getY))
      calculateCenterCoordinates(p1, p2_prime, r2)
    }
  }

  def pairPoints(pointsRDD: SpatialRDD[Point])
    (implicit settings: Settings, logger: Logger): RDD[(Point, Point)] = {

    val epsilon = settings.epsilon
    val circlesRDD = new CircleRDD(pointsRDD, epsilon)
    circlesRDD.spatialPartitioning(pointsRDD.getPartitioner)
    val usingIndex = false
    val considerBoundaryIntersection = true

    JoinQuery.DistanceJoinQueryFlat(pointsRDD, circlesRDD,
      usingIndex, considerBoundaryIntersection).rdd.filter{ case(circle, point) =>
        val id1 = point.getUserData.asInstanceOf[Data].id
        val id2 = circle.getUserData.asInstanceOf[Data].id

        id1 < id2
    }.map{ case(circle, point) => (circle.asInstanceOf[Point], point)}
  }

  def readPoints(input: String, partitions: Int)
    (implicit spark: SparkSession, geofactory: GeometryFactory,
      settings: Settings): SpatialRDD[Point] = {

    val raw = spark.read.option("delimiter", "\t").csv(input).rdd.mapPartitions{ rowIt =>
      rowIt.map{ row =>
        val id = row.getString(0).toInt
        val x =  row.getString(1).toDouble
        val y =  row.getString(2).toDouble
        val t =  row.getString(3).toInt
        val point = geofactory.createPoint(new Coordinate(x, y))
        val data = Data(id, t)
        point.setUserData(data)
        point
      }
    }
    val pointsRDD = new SpatialRDD[Point]
    pointsRDD.setRawSpatialRDD(raw)
    pointsRDD.analyze()

    pointsRDD.spatialPartitioning(GridType.QUADTREE, partitions)
    pointsRDD.spatialPartitionedRDD.rdd.persist(settings.storageLevel)

    pointsRDD
  }

  def readAndReplicatePoints(input: String, partitions: Int)
    (implicit spark: SparkSession, geofactory: GeometryFactory,
      settings: Settings): (RDD[Point], Map[Int, Cell]) = {

    val epsilon = settings.epsilon
    val raw = spark.read.option("delimiter", "\t").csv(input).rdd.mapPartitions{ rowIt =>
      rowIt.map{ row =>
        val id = row.getString(0).toInt
        val x =  row.getString(1).toDouble
        val y =  row.getString(2).toDouble
        val t =  row.getString(3).toInt
        val point = geofactory.createPoint(new Coordinate(x, y))
        val data = Data(id, t)
        point.setUserData(data)
        new Circle(point, epsilon)
      }
    }

    val circlesRDD = new CircleRDD(raw)
    circlesRDD.analyze()

    circlesRDD.spatialPartitioning(GridType.QUADTREE, partitions)

    val points = circlesRDD.spatialPartitionedRDD.rdd
      .map(_.getCenterGeometry.asInstanceOf[Point])
    val cells = circlesRDD.partitionTree.getLeafZones.asScala
      .map{ leaf =>
        val id = leaf.partitionId.toInt
        val lineage = leaf.lineage
        val envelope = leaf.getEnvelope
        (id -> Cell(id, lineage, envelope))
      }.toMap

    (points, cells)
  }

  //** Sequential functions

  def getDisks(points: List[Point], centers: List[Point])
    (implicit settings: Settings): List[Point] = {

    val r  = settings.r
    val mu = settings.mu
    val join = for {
      c <- centers
      p <- points if c.distance(p) <= r
    } yield {
      (c, p)
    }

    join.groupBy(_._1).map{ case(center, points) =>
      val pids = points.map(_._2.getUserData.asInstanceOf[Data].id).sorted.toList
      center.setUserData(pids)
      (center, pids.size)
    }.filter(_._2 >= mu).map(_._1).toList
  }

  def computeCenters(pairs: List[(Point, Point)])
    (implicit geofactory: GeometryFactory, settings: Settings): List[Point] = {

    val r2 = settings.r2
    pairs.map{ case(p1, p2) =>
      calculateCenterCoordinates(p1, p2, r2)
    }.flatten
  }  

  def computePairs(points: List[Point], epsilon: Double): List[(Point, Point)] = {
    for {
      a <- points
      b <- points if {
        val id1 = a.getUserData.asInstanceOf[Data].id
        val id2 = b.getUserData.asInstanceOf[Data].id
          (id1 < id2) && (a.distance(b) <= epsilon)
      }
    } yield {
      (a, b)
    }
  }

  //** Misc

  def saveCells(cells: List[Cell])(implicit geofactory: GeometryFactory): Unit = {
    save("/tmp/edgesCells.wkt"){
      cells.map{ cell =>
        s"${envelope2polygon(cell.envelope).toText}\t${cell.id}\n"
      }
    }
  }

  def envelope2polygon(e: Envelope)
    (implicit geofactory: GeometryFactory): Polygon = {

    val W = e.getMinX()
    val S = e.getMinY()
    val E = e.getMaxX()
    val N = e.getMaxY()
    val WS = new Coordinate(W, S)
    val ES = new Coordinate(E, S)
    val EN = new Coordinate(E, N)
    val WN = new Coordinate(W, N)
    geofactory.createPolygon(Array(WS,ES,EN,WN,WS))
  }

  def save(filename: String)(content: Seq[String]): Unit = {
    val start = clocktime
    val f = new java.io.PrintWriter(filename)
    f.write(content.mkString(""))
    f.close
    val end = clocktime
    val time = "%.2f".format((end - start) / 1000.0)
    logger.info(s"Saved ${filename} in ${time}s [${content.size} records].")
  }

  private def clocktime = System.currentTimeMillis()

  def log(msg: String)(implicit logger: Logger, settings: Settings): Unit = {
    logger.info(s"${settings.appId}|$msg")
  }
}
