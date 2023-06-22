package edu.ucr.dblab.pflock

import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel, Geometry, Envelope}
import com.vividsolutions.jts.geom.{Coordinate, Point, Polygon}
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.serializer.KryoSerializer

import org.datasyslab.geospark.spatialRDD.SpatialRDD
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator

import org.geotools.geometry.jts.{JTS, GeometryClipper}
import com.vividsolutions.jts.index.strtree.STRtree
import com.vividsolutions.jts.index.quadtree.Quadtree

import edu.ucr.dblab.pflock.quadtree.{StandardQuadTree, QuadRectangle, QuadTreePartitioner}

import org.slf4j.{Logger, LoggerFactory}

import collection.JavaConverters._

import Utils._

object MF_Grid {
  case class Tag(pid: Int, gid: Int)
  case class Cell(tag: Tag, mbr: Polygon, pop: Int = 0, id: Int = -1)

  def getGrids(boundary: Polygon, width: Double, prefix: Int = 0)
      (implicit geofactory: GeometryFactory): List[Cell] = {
    val clipper = new GeometryClipper(boundary.getEnvelopeInternal)
    val minX = boundary.getEnvelopeInternal.getMinX
    val minY = boundary.getEnvelopeInternal.getMinY
    val maxX = boundary.getEnvelopeInternal.getMaxX
    val maxY = boundary.getEnvelopeInternal.getMaxY

    val Xs = BigDecimal(minX) to BigDecimal(maxX) by BigDecimal(width)
    val Ys = BigDecimal(minY) to BigDecimal(maxY) by BigDecimal(width)
    val m = Ys.size - 1

    val grids = ( for{ x <- Xs; y <- Ys} yield { (x, y) } ).map{ case(x, y) =>
      val x1 = x.toDouble
      val y1 = y.toDouble
      val x2 = if (x1 + width > maxX) maxX else x1 + width
      val y2 = if (y1 + width > maxY) maxY else y1 + width
      val envelope = new Envelope(x1, x2, y1, y2)
      val grid = JTS.toGeometry(envelope, geofactory)

      val p = grid.getCentroid
      val i = math.floor( (p.getX - minX) / width ).toInt
      val j = math.floor( (p.getY - minY) / width ).toInt
      val gid = i + j * m
      val tag = Tag(prefix, gid)
       
      Cell(tag, grid)
    }.toList

    grids
  }

  def main(args: Array[String]) = {
    implicit val params = new Params(args)
    implicit val spark = SparkSession.builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .config("spark.kryoserializer.buffer.max", "256m")
      .appName("MF_Grid").getOrCreate()
    import spark.implicits._
    implicit val settings = Settings(
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      tolerance = params.tolerance(),
      seed = params.seed(),
      debug = params.debug(),
      appId = spark.sparkContext.applicationId,
      storageLevel = params.storage() match {
        case 1 => StorageLevel.MEMORY_ONLY_SER_2
        case 2 => StorageLevel.NONE
        case _ => StorageLevel.MEMORY_ONLY_2  // 0 is the default...
      }
    )
    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))
    import org.apache.spark.sql.Encoders
    val schema = Encoders.product[ST_Point].schema
    val command = System.getProperty("sun.java.command")
    log(s"$command")

    val input = params.input()
    val rows = spark.read.option("header", "false").option("delimiter", "\t")
      .schema(schema).csv(input).as[ST_Point]

    log(s"Point from input: ${rows.count()}")

    val width = params.width()
    val epsilon = params.epsilon()
    val pointsRaw = rows.rdd.map(_.point)
    val pointsRDD = new SpatialRDD[Point]()
    pointsRDD.setRawSpatialRDD(pointsRaw)
    pointsRDD.analyze
    val boundary = new QuadRectangle(pointsRDD.boundary)
    val maxentries = params.maxentries()
    val fraction   = params.fraction()
    val maxlevel   = params.maxlevel()

    val quadtree = new StandardQuadTree[Point](boundary, 0, maxentries, maxlevel)
    val samples = pointsRaw.sample(false, 1.0, params.seed()).collect
    samples.foreach{ sample =>
      quadtree.insert(new QuadRectangle(sample.getEnvelopeInternal), sample)
    }
    quadtree.assignPartitionIds()
    quadtree.assignPartitionLineage()

    log(s"Quadtree cells: ${quadtree.getLeafZones.size()}")

    val partitioner = new QuadTreePartitioner(quadtree)
    pointsRDD.spatialPartitioning(partitioner)

    val threshold = params.threshold()
    val parts = pointsRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ (pid, it) =>
      val n = it.size
      Iterator((pid, n))
    }.collect.toList
    val zones = quadtree.getLeafZones.asScala.map(z => (z.partitionId, z)).toList

    val cells = for{
      p <- parts
      z <- zones if z._1 == p._1 
    } yield {
      val pid = z._1
      val mbr = JTS.toGeometry(z._2.getEnvelope, geofactory)
      val pop = p._2
      Cell(Tag(pid, 0), mbr, pop, pid)
    }

    val cids = cells.filter(_.pop > threshold).map(_.id).toSet

    // START: Testing cells...
    val grids_prime = cells.filter(c => cids.contains(c.id)).map{ cell =>
      getGrids(cell.mbr, width, cell.id)
    }.flatten
    val cells_prime = cells.filterNot(c => cids.contains(c.id))

    val C_prime = grids_prime ++ cells_prime
    val C = C_prime.sortBy{ c =>
      (c.tag.pid, c.tag.gid)
    }.zip(0 until C_prime.size).map{ case(c, index) => c.copy(id = index)}

    save("/tmp/edgesC.wkt"){
      C.map{ cell =>
        val wkt = cell.mbr.toText
        val id  = cell.id
        val n   = cell.pop
        val i   = s"${cell.tag.pid}_${cell.tag.gid}"

        s"$wkt\t$id\t$n\t$i\n"
      } 
    }
    // END: Testing cells...

    val tree = new STRtree()
    C.foreach{ cell =>
      tree.insert(cell.mbr.getEnvelopeInternal, cell.id)
    }
    val points2RDD = pointsRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ (pid, points) =>
      points.flatMap { point =>
        val envelope = point.getEnvelopeInternal
        envelope.expandBy(epsilon / 2.0)
        tree.query(envelope).asScala.map{ id => (id.asInstanceOf[Int], point)}.toIterator
      }
    }
    //**

    save("/tmp/edgesR.wkt"){
      points2RDD.map{ case(pid, p) =>
        val wkt  = p.toText
        val data = p.getUserData.toString()
        s"$wkt\t$data\t$pid\n"
      }.collect
    }

    spark.close()
  }
}
