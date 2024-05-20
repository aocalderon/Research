package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.MF_Utils._
import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.sedona.quadtree.{QuadRectangle, StandardQuadTree}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom._
import org.locationtech.jts.index.quadtree.{Quadtree => JTSQuadtree}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.xml._

object Tester {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params = new BFEParams(args)

    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .master(params.master())
      .appName("Tester").getOrCreate()
    import spark.implicits._

    implicit val S = Settings(
      dataset = params.dataset(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = "PFlocks",
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
    val trajs = spark.read
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
      }.filter{ p => p._4 == S.endtime }.cache

    val nTrajs = trajs.count()
    log(s"Number of trajectories: $nTrajs")

    val sample = trajs.sample(withReplacement = false, fraction = params.fraction(), seed = 42)
      .map{ case(_, x, y, _) =>
        new Envelope(new Coordinate(x, y))
      }.collect()
    val envelope = PF_Utils.getEnvelope2(trajs.map(p => G.createPoint(new Coordinate(p._2, p._3))))
    envelope.expandBy(S.epsilon)
    val quadtree = new StandardQuadTree[Int](new QuadRectangle(envelope), 0, params.capacity(), 16)
    sample.foreach { env =>
      quadtree.insert(new QuadRectangle(env), 1)
    }
    quadtree.assignPartitionIds()
    quadtree.assignPartitionLineage()
    quadtree.dropElements()
    val cells = quadtree.getLeafZones.asScala.map { leaf =>
      val envelope = leaf.getEnvelope
      val id = leaf.partitionId.toInt

      id -> Cell(envelope, id, leaf.lineage)
    }.toMap
    val ncells = cells.size

    save(s"/home/acald013/Research/local_path/${S.dataset}/Q_P${ncells}_C${S.capacity}_F${(S.fraction * 100).toInt}.wkt") {
      cells.map { case(id, cell) =>
        val wkt = G.toGeometry(cell.mbr).toText
        s"$wkt\tL${cell.lineage}\t$id\n"
      }.toList
    }

    val trajs_partitioned = trajs.mapPartitions { rows =>
      rows.flatMap { p =>
        val env = new Envelope(new Coordinate(p._2,p._3))
        env.expandBy(S.epsilon)
        quadtree.findZones(new QuadRectangle(env)).asScala.map{ x => (x.partitionId, p) }
      }
    }.partitionBy( SimplePartitioner( quadtree.getLeafZones.size() ) ).map(_._2).cache

    log("Start.")
    val t0 = clocktime
    val MaximalsRDD = trajs_partitioned.mapPartitionsWithIndex { (index, points) =>
      val cell = cells(index).mbr
      val ps = points.toList.map { case(o,x,y,t) =>
        val data = Data(o, t)
        val point = G.createPoint(new Coordinate(x, y))
        point.setUserData(data)
        STPoint(point)
      }
      val (maximals, _) = if( S.method == "BFE") BFE.run(ps) else PSI.run(ps)

      maximals.filter{ maximal => cell.contains(maximal.center.getCoordinate) }.toIterator
    }
    val nMaximals = MaximalsRDD.count()
    val tMaximals = (clocktime - t0) / 1e9
    logt(s"$ncells|${S.capacity}|${S.fraction}|$tMaximals")
    log(s"$ncells|$nMaximals")

    /*******************************************************************************/

    spark.close()

    log(s"END|")
  }
}
