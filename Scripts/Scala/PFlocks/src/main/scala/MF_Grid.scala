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

import edu.ucr.dblab.pflock.quadtree.{StandardQuadTree, QuadRectangle, QuadTreePartitioner}

import org.slf4j.{Logger, LoggerFactory}

import collection.JavaConverters._

import Utils._

object MF_Grid {
  case class Cell(cid: String, mbr: Polygon, pop: Int, id: Int = -1)

  def getGrids(boundary: Polygon, points: List[Point], width: Double, prefix: Int = 0): List[Cell] = {
    def digits(x: Int) = math.ceil( math.log( math.abs(x) + 1 ) / math.log(10) ).toInt
    val clipper = new GeometryClipper(boundary.getEnvelopeInternal)
    val minX = boundary.getEnvelopeInternal.getMinX
    val minY = boundary.getEnvelopeInternal.getMinY
    val maxX = boundary.getEnvelopeInternal.getMaxX
    val maxY = boundary.getEnvelopeInternal.getMaxY
    val Xs = BigDecimal(minX) to BigDecimal(maxX) by BigDecimal(width)
    val Ys = BigDecimal(minY) to BigDecimal(maxY) by BigDecimal(width)
    val m = Ys.size - 1

    val grids = (for{ x <- Xs; y <- Ys} yield { (x, y) }).map{ coord =>
      val x = coord._1.toDouble
      val y = coord._2.toDouble
      val envelope = new Envelope(x, x + width, y, y + width)

      val grid = clipper.clipSafe(JTS.toGeometry(envelope), true, 0.001).asInstanceOf[Polygon]

      val point = grid.getCentroid
      val i = math.floor( (point.getX - minX) / width).toInt
      val j = math.floor( (point.getY - minY) / width).toInt
      val id = i + j * m
      
      grid.setUserData(id)
      grid
    }.toList

    val pops = points.map{ point =>
      val grid = grids.filter(_.contains(point))
      if(!grid.isEmpty){
        grid.head.getUserData.asInstanceOf[Int]
      } else {
        -1
      }
    }.foldLeft(Map.empty[Int, Int]){ (count, cid) =>
      count + (cid -> (count.getOrElse(cid, 0) + 1))
    }

    grids.map{ grid =>
      val cid = grid.getUserData.asInstanceOf[Int]

      Cell(s"${prefix}_${cid}", grid, -1)
    }
  }

  def allocateGrid(boundary: Polygon, points: List[Point], width: Double, epsilon: Double, pid: Int = -1): List[(String, Point)] = {
    case class Key(i: Int, j: Int)
    case class KeyPoint(key: Key, point: Point)

    val minX = boundary.getEnvelopeInternal.getMinX
    val minY = boundary.getEnvelopeInternal.getMinY
    val maxX = boundary.getEnvelopeInternal.getMaxX
    val maxY = boundary.getEnvelopeInternal.getMaxY
    val w = math.floor((maxX - minX) / width).toInt
    val h = math.floor((maxY - minY) / width).toInt
    println(s"PID: $pid => $w x $h")

    val keys_points = points.flatMap{ point =>
      val i_start = math.floor(( (point.getX - minX) - epsilon) / width).toInt
      val i_end   = math.floor(( (point.getX - minX) + epsilon) / width).toInt
      val is = i_start to i_end
      val j_start = math.floor(( (point.getY - minY) - epsilon) / width).toInt
      val j_end   = math.floor(( (point.getY - minY) + epsilon) / width).toInt
      val js = j_start to j_end

      val keys = for{ 
        i <- is
        j <- js if 0 <= i && i <= w && 0 <= j && j <= h
      } yield {
        Key(i, j)
      }

      keys.map(key => KeyPoint(key, point))
    }

    val grids = keys_points.map{ p =>
      val key = p.key.i + p.key.j * w
      (s"${pid}_${key}", p.point)
    }

    println(s"$pid:")
    grids.foreach{println}
    println

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

    //**
    val threshold = 50
    val parts = pointsRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{ (pid, it) =>
      val n = it.size
      Iterator((pid, n))
    }.collect.toList
    val zones = quadtree.getLeafZones.asScala.map(z => (z.partitionId, z)).toList

    val cells = for{
      p <- parts
      z <- zones if z._1 == p._1 
    } yield {
      val cid = z._1
      val mbr = JTS.toGeometry(z._2.getEnvelope, geofactory)
      val pop = p._2
      Cell(cid.toString, mbr, pop, cid)
    }

    val cids = cells.filter(_.pop > threshold).map(_.cid.toInt).toSet

    val points2RDD = pointsRDD.spatialPartitionedRDD.rdd.mapPartitionsWithIndex{(pid, it) =>
      if(cids.contains(pid)){
        val points = it.toList
        val boundary = cells.filter(_.cid.toInt == pid).head.mbr
        allocateGrid(boundary, points, width, epsilon, pid).map( p => (p._1, p._2)).toIterator
      } else {
        it.map( p => (s"${pid}_0", p))
      }
    }
    val keys = points2RDD.map(_._1).distinct.collect.sortBy{ key =>
        val coords = key.split("_").map(_.toInt)
        val i = coords(0)
        val j = coords(1)

        (i, j)
    }
    val index = keys.zip(0 until keys.size).map(p => p._1 -> p._2).toMap
    index.toList.sortBy(_._2).foreach(println)

    val points3RDD = points2RDD.map{ case(key, point) =>
      (index(key), point)
    }
    //**

    val n = cells.size - cids.size
    save("/tmp/edgesG.wkt"){
      val grids_prime = cells.filter(c => cids.contains(c.cid.toInt)).map{ cell =>
        getGrids(cell.mbr, pointsRaw.collect.toList, width, cell.cid.toInt)
      }.flatten.sortBy(_.cid).zipWithIndex.map{ case(cell, index) =>
        cell.copy(id = n + index)
      }
      val cells_prime = cells.filterNot(c => cids.contains(c.cid.toInt)).
        sortBy(_.cid).zipWithIndex.map{ case(cell, index) =>
          cell.copy(id = index)
        }


      (cells_prime ++ grids_prime).map{ cell =>
        val wkt = cell.mbr.toText
        val id  = cell.cid
        val n   = cell.pop
        val i   = cell.id

        s"$wkt\t$id\t$n\t$i\n"
      } 
    }
    save("/tmp/edgesR.wkt"){
      points3RDD.map{ case(pid, p) =>
        val wkt  = p.toText
        val data = p.getUserData.toString()
        s"$wkt\t$data\t$pid\n"
      }.collect
    }

    spark.close()
  }
}
