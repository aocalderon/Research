package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.MF_Utils.SimplePartitioner
import edu.ucr.dblab.pflock.Utils._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom._
import org.locationtech.jts.io.WKTReader
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.io.Source

object MF_Tester {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params = new BFEParams(args)

    implicit val spark: SparkSession = SparkSession.builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .master(params.master())
      .appName("Tester").getOrCreate()
    import spark.implicits._

    implicit val S: Settings = Settings(
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

    implicit val G: GeometryFactory = new GeometryFactory(new PrecisionModel(S.scale))

    printParams(args)
    log(s"START|")

    /*******************************************************************************/
    // Code here...
    val points = spark.read
      .option("header", value = false)
      .option("delimiter", "\t")
      .csv(S.dataset)
      .rdd
      .mapPartitions { rows =>
        rows.map { row =>
          val oid = row.getString(0).toInt
          val lon = row.getString(1).toDouble
          val lat = row.getString(2).toDouble
          val tid = row.getString(3).toInt

          (oid, lon, lat, tid)

        }
      }.filter(_._4 == 15).cache

    case class Cell(mbr: Envelope, id: Int)
    val reader = new WKTReader(G)
    val buffer = Source.fromFile("/home/acald013/Research/local_path/PFlock/LA/LA_25K/quadtree.wkt")
    val cells = buffer.getLines().map{ line =>
      val arr = line.split("\\t")
      val mbr = reader.read(arr(0)).getEnvelopeInternal
      val id = arr(2).toInt
      Cell(mbr, id)
    }.toList
    buffer.close()
    implicit val cell_mbrs: Map[Int, Cell] = cells.map(c => (c.id, c)).toMap

    val tree = new org.locationtech.jts.index.strtree.STRtree()
    cells.foreach{ cell =>
      tree.insert(cell.mbr, cell)
    }

    val pointsRDD = points.flatMap{ point =>
      val coord = new Coordinate(point._2, point._3)
      val P = G.createPoint(coord)
      val data = Data(point._1, point._4)
      P.setUserData(data)
      val envelope = P.getEnvelopeInternal
      envelope.expandBy(S.epsilon)
      val cells = tree.query(envelope).asScala.map(c => c.asInstanceOf[Cell])
      cells.map { cell =>
        (cell.id, STPoint(P))
      }
    }.partitionBy(SimplePartitioner(cells.size)).map(_._2).cache()

    save("/tmp/edgesP.wkt"){
      pointsRDD.mapPartitionsWithIndex{ (index, points) =>
        val P = points.toList
        val n = P.length
        P.map{ point =>
          s"${point.wkt}\t$index\t$n\n"
        }.toIterator
      }.collect
    }

    val stats = pointsRDD.mapPartitionsWithIndex{ (index, points) =>
      val wkt = G.toGeometry(cell_mbrs(index).mbr).toText
      var t0 = clocktime
      val (m1, s1) = BFE.run(points.toList)
      val t1 = (clocktime - t0) / 1e9
      t0 = clocktime
      val (m2, s2) = PSI.run(points.toList)
      val t2 = (clocktime - t0) / 1e9

      Iterator(s"$wkt\t$index\t${t1/4.0}\t$t2\n")
    }

    save("/tmp/edgesCells.wkt") {
      cells.map{ cell =>
        val wkt = G.toGeometry(cell.mbr).toText
        val id = cell.id
        s"$wkt\t$id\n"
      }
    }
    /*******************************************************************************/

    spark.close()

    log(s"END|")
  }
}
