package edu.ucr.dblab

import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, CircleRDD, PointRDD}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.enums.{GridType, IndexType, FileDataSplitter}
import com.vividsolutions.jts.geom.{GeometryFactory, Coordinate}

object GeoTesterRDD_Join{

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("GeoTester_Join")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    val pointRDDInputLocation = "/Datasets/Demo/data.tsv"
    val pointRDDOffset = 1
    val pointRDDSplitter = FileDataSplitter.TSV
    val carryOtherAttributes = true 
    val pointsRDD = new PointRDD(spark.sparkContext, pointRDDInputLocation, pointRDDOffset, pointRDDSplitter, carryOtherAttributes)

    val distance = 10.0

    pointsRDD.spatialPartitioning(GridType.QUADTREE, 10)
    pointsRDD.spatialPartitionedRDD.cache
    val npartitions = pointsRDD.getPartitioner.getGrids.size()
    val buffersRDD = new CircleRDD(pointsRDD, distance)
    buffersRDD.analyze()
    buffersRDD.spatialPartitioning(pointsRDD.getPartitioner)
    buffersRDD.spatialPartitionedRDD.cache

    pointsRDD.partitionTree.getLeafZones.asScala.map{ z =>
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

    // Finding pairs and centers...
    def calculateCenterCoordinates(p1: Point, p2: Point, r2: Double): (Point, Point) = {
      var h = geofactory.createPoint(new Coordinate(-1.0,-1.0))
      var k = geofactory.createPoint(new Coordinate(-1.0,-1.0))
      val X: Double = p1.getX - p2.getX
      val Y: Double = p1.getY - p2.getY
      val D2: Double = math.pow(X, 2) + math.pow(Y, 2)
      if (D2 != 0.0){
        val root: Double = math.sqrt(math.abs(4.0 * (r2 / D2) - 1.0))
        val h1: Double = ((X + Y * root) / 2) + p2.getX
        val k1: Double = ((Y - X * root) / 2) + p2.getY
        val h2: Double = ((X - Y * root) / 2) + p2.getX
        val k2: Double = ((Y + X * root) / 2) + p2.getY
        h = geofactory.createPoint(new Coordinate(h1,k1))
        k = geofactory.createPoint(new Coordinate(h2,k2))
      }
      (h, k)
    }
    val r2: Double = math.pow(params.epsilon() / 2.0, 2)
    val considerBoundary = true
    val stageA = "A.Pairs and centers found"
    val (centers, nCenters) = timer{header(stageA)}{
      val usingIndex = false
      val centersPairs = JoinQuery.DistanceJoinQueryFlat(points, buffers, usingIndex, considerBoundary)
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
      }.persist(StorageLevel.MEMORY_ONLY)
      val centers = centersPairs.map(_._1)
        .union(centersPairs.map(_._2))
        .persist(StorageLevel.MEMORY_ONLY)
      val nCenters = centers.count()
      n(stageA, nCenters)
      (centers, nCenters)
    }

    //
    val radius = (params.epsilon() / 2.0) + params.precision()
    save("/tmp/edgesCenters.wkt"){
      centers.map{ center =>
        s"${center.buffer(radius, 10).toText()}\t${center.getX},${center.getY}\n"
      }.collect()
    }

    ///////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////

    val stageP2 = "Partitions 2 done"
    val (points2, buffers2, npartitions2) = timer{stageP2}{
      pointsRDD.spatialPartitioning(gridtype, params.partitions())
      pointsRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)
      val npartitions = pointsRDD.getPartitioner.getGrids.size()
      val buffersRDD = new CircleRDD(pointsRDD, distance)
      buffersRDD.analyze(envelope, nPointsRDD.toInt)
      buffersRDD.spatialPartitioning(pointsRDD.getPartitioner)
      n(stageP2, buffersRDD.spatialPartitionedRDD.count())
      (pointsRDD, buffersRDD, npartitions)
    }
    logger.info(s"GridType: $gridtype.")
    save{"/tmp/edgesCells2.wkt"}{
      points2.partitionTree.getLeafZones.asScala.map{ z =>
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
    }

    val stageB = "B.Pairs and centers found"
    val (centers2, nCenters2) = timer{header(stageB)}{
      val usingIndex = false
      val centersPairs = JoinQuery.DistanceJoinQueryFlat(points2, buffers2, usingIndex, considerBoundary)
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
      }.persist(StorageLevel.MEMORY_ONLY)
      val centers = centersPairs.map(_._1)
        .union(centersPairs.map(_._2))
        .persist(StorageLevel.MEMORY_ONLY)
      val nCenters = centers.count()
      n(stageB, nCenters)
      (centers, nCenters)
    }

    //
    save("/tmp/edgesCenters2.wkt"){
      centers.mapPartitionsWithIndex( (index, centers) =>
        centers.map{ center =>
          s"${center.toText()}\t${center.getX},${center.getY}\t${index}\n"
        }
      , preservesPartitioning = true ).collect()
    }
  }
}

class GTJConf(args: Seq[String]) extends ScallopConf(args) {
  val input = opt[String](default = Some(""))
  val epsilon = opt[Double](default = Some(10.0))
  val mu = opt[Int](default = Some(2))
  val precision = opt[Double](default = Some(0.001))
  val partitions = opt[Int](default = Some(256))
  val parallelism = opt[Int](default = Some(324))
  val gridtype = opt[String](default = Some("quadtree"))
  val indextype = opt[String](default = Some("none"))

  verify()
}
