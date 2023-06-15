package edu.ucr.dblab.dstester

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point}

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.io.Source

import org.apache.commons.math3.stat.clustering.{EuclideanDoublePoint, DBSCANClusterer => ApacheDBScan}
import smile.clustering.{DBSCAN => SmileDBScan}
import edu.ucr.dblab.dstester.spmf.{DoubleArray, AlgoDBSCAN => SpmfDBScan}
import com.esri.dbscan.{DBSCANIndex, DBSCAN => EsriDBScan, DBSCANPoint2D}

import elki.clustering.dbscan.{DBSCAN => ElkiDBScan}
import elki.index.tree.spatial.rstarvariants.rstar.RStarTreeFactory
import elki.index.tree.spatial.rstarvariants.RTreeSettings
import elki.persistent.MemoryPageFileFactory
import elki.database.StaticArrayDatabase
import elki.datasource.ArrayAdapterDatabaseConnection
import elki.distance.minkowski.EuclideanDistance
import elki.data.DoubleVector
import elki.data.`type`.TypeUtil

import elki.clustering.subspace.CLIQUE
import elki.data.model.SubspaceModel

object DSTester {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params = new Params(args)
    implicit val geofactory = new GeometryFactory(new PrecisionModel(
      1.0 / params.tolerance()
    ))

    val points = readPoints(params.input())
    val epsilon = params.epsilon()
    val mu = params.mu()

    /*** Tests ***/
    val (cElki, tElki) = timer{
      val data = points.map{ point =>
        Array(point.getX, point.getY)
      }.toArray
      val dbc = new ArrayAdapterDatabaseConnection(data)
      val indexfactory = new RStarTreeFactory(new MemoryPageFileFactory(512), new RTreeSettings())
      val db = new StaticArrayDatabase(dbc, List(indexfactory).asJava);
      db.initialize()
      val dist = EuclideanDistance.STATIC
      val dbscan = new ElkiDBScan(dist, epsilon, mu)
      val clusters  = dbscan.autorun(db).getAllClusters().asScala.toList

      val rel = db.getRelation(TypeUtil.NUMBER_VECTOR_FIELD);
      clusters.zipWithIndex.map{ case(cluster, id) =>
        val iter = cluster.getIDs().iter()
        val points = scala.collection.mutable.ListBuffer[Point]()
        while(iter.valid()){
          val value = rel.get(iter).asInstanceOf[DoubleVector]
          val coord = new Coordinate(value.doubleValue(0),value.doubleValue(1))
          points.append(geofactory.createPoint(coord))
          iter.advance()
        }
        (points.toList, id)
      }
    }
    val nElki = cElki.size
    logt(s"$epsilon|$mu|Elki|$nElki|$tElki")
    saveCluster("Elki", cElki)

    val (cEsri, tEsri) = timer{
      case class JTSPoint(id: Long, point: Point) extends DBSCANPoint2D {
        def x: Double = point.getX
        def y: Double = point.getY
      }
      val dataEsri = points.zipWithIndex.map{ case(point, id) => JTSPoint(id, point)}.toArray
      val si = dataEsri.foldLeft(DBSCANIndex[JTSPoint](epsilon))(_ + _)
      EsriDBScan(mu, si).clusters(dataEsri).map{ cluster =>
        (cluster.points.map(_.point).toList, cluster.id)
      }.toList
    }
    val nEsri = cEsri.size
    logt(s"$epsilon|$mu|Esri|$nEsri|$tEsri")
    saveCluster("Esri", cEsri)

    val (cSpmf, tSpmf) = timer{
      val dataSpmf = points.map{ point =>
        val vector = Array[Double](point.getX, point.getY)
        new DoubleArray(vector)
      }.asJava
      
      val algo = new SpmfDBScan()
      
      algo.run(dataSpmf, mu, epsilon).asScala.zipWithIndex.toList
        .map{ case(cluster, id) =>
          val points = cluster.getVectors.asScala.map{ c =>
            geofactory.createPoint(new Coordinate(c.get(0), c.get(1)))
          }.toList
          (points, id)
        }
    }
    val nSpmf = cSpmf.size
    logt(s"$epsilon|$mu|Spmf|$nSpmf|$tSpmf")
    saveCluster("Spmf", cSpmf)

    val (cSmile, tSmile) = timer{
      val dataSmile = points.map{ point =>
        Array(point.getX, point.getY)
      }.toArray
      val model = SmileDBScan.fit(dataSmile, mu, epsilon)

      model.y.zip(points).groupBy(_._1).map{ case(id, points) =>
        (points.toList.map(_._2), id)
      }.toList
    }
    val nSmile = cSmile.size
    logt(s"$epsilon|$mu|Smile|$nSmile|$tSmile")
    saveCluster("Smile", cSmile)

    val (cApache, tApache) = timer{
      val dataApache = points.map{ point =>
        new EuclideanDoublePoint(Array(point.getX, point.getY))
      }.asJava
      val model: ApacheDBScan[EuclideanDoublePoint] = new ApacheDBScan(epsilon, mu)
      val clusters = model.cluster(dataApache).asScala.toList
        
      clusters.map{ cluster =>
        cluster.getPoints.asScala.map{ edp =>
          val p = edp.getPoint()
          geofactory.createPoint(new Coordinate(p(0), p(1)))
        }.toList
      }.zipWithIndex
    }
    val nApache = cApache.size
    logt(s"$epsilon|$mu|Apache|$nApache|$tApache")
    saveCluster("Apache", cApache)

  }

  def readPoints(input: String)
    (implicit geofactory: GeometryFactory): List[Point] = {

    val buffer = Source.fromFile(input)
    val points = buffer.getLines.toList
      .map{ line =>
        val arr = line.split("\t")
        val i = arr(0).toInt
        val x = arr(1).toDouble
        val y = arr(2).toDouble
        val t = arr(3).toInt
        val point = geofactory.createPoint(new Coordinate(x, y))
        point.setUserData(i)
        point
      }
    buffer.close
    points
  }

  def saveCluster(name: String, clusters: List[(List[Point], Int)]): Unit = {
    save(s"/tmp/edges${name}.wkt"){
      clusters.map{ case(points, id) =>
        points.map{ p => s"${p.toText()}\t$id\n"}
      }.flatten
    }
  }

  def clocktime: Long = System.nanoTime()

  def log(msg: String)(implicit logger: Logger): Unit = {
    logger.info(s"INFO|$msg")
  }

  def logt(msg: String)(implicit logger: Logger): Unit = {
    logger.info(s"TIME|$msg")
  }

  def timer[R](block: => R): (R, Double) = {
    val t0 = clocktime
    val result = block    // call-by-name
    val t1 = clocktime
    val time = (t1 - t0) / 1e9
    (result, time)
  }

  def save(filename: String)(content: Seq[String])(implicit logger: Logger): Unit = {
    val start = clocktime
    val f = new java.io.PrintWriter(filename)
    f.write(content.mkString(""))
    f.close
    val end = clocktime
    val time = "%.2f".format((end - start) / 1e9)
    logger.info(s"Saved ${filename} in ${time}s [${content.size} records].")
  }  
}

import org.rogach.scallop._

class Params(args: Seq[String]) extends ScallopConf(args) {
  val filename = "/home/acald013/Research/Datasets/dense2.tsv"
  val input:     ScallopOption[String]  = opt[String]  (default = Some(filename))
  val epsilon:   ScallopOption[Double]  = opt[Double]  (default = Some(10.0))
  val mu      :  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val tolerance: ScallopOption[Double]  = opt[Double]  (default = Some(1e-3))

  verify()
}
