package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.MF_Utils._
import edu.ucr.dblab.pflock.Utils._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom._
import org.locationtech.jts.index.quadtree.{Quadtree => JTSQuadtree}
import org.locationtech.jts.io.WKTReader
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.io.Source

object Tester {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params: BFEParams = new BFEParams(args)

    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.driver.memory","35g")
      .config("spark.executor.memory","20g")
      .config("spark.memory.offHeap.enabled",true)
      .config("spark.memory.offHeap.size","16g")
      .config("spark.driver.maxResultSize","4g")
      .config("spark.kryoserializer.buffer.max","256m")
      .master(params.master())
      .appName("Tester").getOrCreate()
    import spark.implicits._

    implicit val S = Settings(
      dataset = params.dataset(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = params.method(),
      capacity = params.capacity(),
      fraction = params.fraction(),
      tolerance = params.tolerance(),
      tag = params.tag(),
      debug = params.debug(),
      output = params.output(),
      appId = spark.sparkContext.applicationId
    )

    implicit val G = new GeometryFactory(new PrecisionModel(S.scale))

    printParams(args)
    log(s"START|")

    /*******************************************************************************/
    // Code here...
    val path = "file:///home/acald013/Datasets/GeoInformatica/mainus/"
    //val path = "gadm/l3vsl2"
    val A = read(s"$path/A.wkt").cache
    println(A.count)
    //val B = read(s"$path/B.wkt").cache

    case class Cell(mbr: Envelope, id: Int)
    val reader = new WKTReader(G)
    val buffer = Source.fromFile(s"/home/acald013/Datasets/GeoInformatica/mainus/quadtree.wkt")
    val cells = buffer.getLines().map{ line =>
      val arr = line.split("\\t")
      val mbr = reader.read(arr(2)).getEnvelopeInternal
      val id = arr(1).toInt
      Cell(mbr, id)
    }.toList
    buffer.close()
    implicit val cell_mbrs: Map[Int, Cell] = cells.map(c => (c.id, c)).toMap

    val tree = new org.locationtech.jts.index.strtree.STRtree()
    cells.foreach{ cell =>
      tree.insert(cell.mbr, cell)
    }

    val ARDD = A.mapPartitions{ edges =>
      edges.flatMap { edge =>
        val envelope = edge.getEnvelopeInternal
        val cells = tree.query(envelope).asScala.map(c => c.asInstanceOf[Cell]).toList
        cells.map { pid =>
          (pid.id, edge)
        }
      }
    }.partitionBy(SimplePartitioner(cells.size)).map(_._2).cache()

    val nA = ARDD.count()

    def getBorders(mbr: Envelope): LineString = {
      val p1 = new Coordinate(mbr.getMinX, mbr.getMinY)
      val p2 = new Coordinate(mbr.getMaxX, mbr.getMinY)
      val p3 = new Coordinate(mbr.getMaxX, mbr.getMaxY)
      val p4 = new Coordinate(mbr.getMinX, mbr.getMaxY)
      val coords = Array(p1,p2,p3,p4)
      G.createLineString(coords)
    }
    val n = ARDD.mapPartitionsWithIndex{ (index, edges) =>
      val borders = getBorders(cell_mbrs(index).mbr)
      val n = edges.filter{ edge =>
        edge.intersects(borders)
      }.length
      Iterator(n)
    }.filter(_ > 0).count

    println(nA)
    println(cells.length - n)
    println(n)

    /*******************************************************************************/

    spark.close()

    log(s"END|")
  }

  def read(input: String)(implicit spark: SparkSession, G: GeometryFactory, S: Settings): RDD[LineString] = {

    val polys = spark.read.textFile(input).rdd.cache()
    debug{
      val nPolys = polys.count
      log(s"INFO|npolys=$nPolys")
    }
    val edgesRaw = polys.mapPartitions{ lines =>
      val reader = new WKTReader(G)
      lines.flatMap{ line =>
        val arr = line.split("\t")
        val wkt = arr(0)
        val id  = arr(1)
        val geom = reader.read(wkt)

        geom match {
          case _: Polygon =>
            (0 until geom.getNumGeometries).map { i =>
              (geom.getGeometryN(i).asInstanceOf[Polygon], id)
            }.flatMap { case (polygon, id) =>
              getLineStrings(polygon, id)
            }.toIterator
          case _: MultiPolygon =>
            val polygon = geom.getGeometryN(0).asInstanceOf[Polygon]
            getLineStrings(polygon, id)

          case _: LineString =>
            val coords = geom.getCoordinates
            coords.zip(coords.tail).zipWithIndex.map { case (pair, order) =>
              val coord1 = pair._1
              val coord2 = pair._2
              val coords = Array(coord1, coord2)
              val isHole = false
              val n = geom.getNumPoints - 2

              val line = G.createLineString(coords)
              // Save info from the edge...
              line.setUserData(s"$id\t0\t$order\t$isHole\t$n")

              line
            }
          case _ =>
            println(s"$id\tNot a valid geometry")
            List.empty[LineString]
        }
      }
    }.cache()

    edgesRaw
  }

  private def getLineStrings(polygon: Polygon, polygon_id: String)(implicit geofactory: GeometryFactory): List[LineString] = {
    getRings(polygon)
      .zipWithIndex
      .flatMap{ case(ring, ring_id) =>
        ring.zip(ring.tail).zipWithIndex.map{ case(pair, order) =>
          val coord1 = pair._1
          val coord2 = pair._2
          val coords = Array(coord1, coord2)
          val isHole = ring_id > 0
          val n = polygon.getNumPoints - 2

          val line = geofactory.createLineString(coords)
          // Save info from the edge...
          line.setUserData(s"$polygon_id\t$ring_id\t$order\t$isHole\t$n")

          line
        }
      }
  }

  import org.locationtech.jts.algorithm.CGAlgorithms
  private def getRings(polygon: Polygon): List[Array[Coordinate]] = {
    val exterior_coordinates = polygon.getExteriorRing.getCoordinateSequence.toCoordinateArray
    val outerRing = if(!CGAlgorithms.isCCW(exterior_coordinates)) { exterior_coordinates.reverse } else { exterior_coordinates }

    val nInteriorRings = polygon.getNumInteriorRing
    val innerRings = (0 until nInteriorRings).map{ i =>
      val interior_coordinates = polygon.getInteriorRingN(i).getCoordinateSequence.toCoordinateArray
      if(CGAlgorithms.isCCW(interior_coordinates)) { interior_coordinates.reverse } else { interior_coordinates }
    }.toList

    outerRing +: innerRings
  }

  def getStudyArea(edges: RDD[LineString]): Envelope = {
    edges.map(_.getEnvelopeInternal).reduce { (a, b) =>
      a.expandToInclude(b)
      a
    }
  }

  def saveEdgesRDD(path: String, edgesRDD: RDD[LineString]): Unit = {
    save(path) {
      edgesRDD.mapPartitionsWithIndex { (cid, edges) =>
        edges.map { edge =>
          val wkt = edge.toText
          val dat = edge.getUserData

          s"$wkt\t$cid\t$dat\n"
        }
      }.collect
    }
  }

  def debug[R](block: => R)(implicit S: Settings): Unit = { if(S.debug) block }

}
