package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.MF_Utils.SimplePartitioner
import edu.ucr.dblab.pflock.Utils._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, PrecisionModel}
import org.locationtech.jts.index.strtree.STRtree
import org.locationtech.jts.io.WKTReader
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.io.Source

object PFlock3 {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params: BFEParams = new BFEParams(args)

    implicit val spark: SparkSession = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .master(params.master())
      .appName("PFlock").getOrCreate()

    implicit val S: Settings = Settings(
      dataset = params.dataset(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      delta = params.delta(),
      sdist = params.sdist(),
      capacity = params.capacity(),
      fraction = params.fraction(),
      tolerance = params.tolerance(),
      tag = params.tag(),
      debug = params.debug(),
      print = params.print(),
      output = params.output(),
      iindex = params.iindex(),
      method = params.method(),
      appId = spark.sparkContext.applicationId
    )

    implicit val G: GeometryFactory = new GeometryFactory(new PrecisionModel(S.scale))

    printParams(args)
    log(s"START|")

    /*******************************************************************************/
    // Code here...

    val trajs = spark.read
      .option("header", value = false)
      .option("delimiter", "\t")
      .csv(S.dataset)
      .rdd
      .mapPartitions{ rows =>
        rows.map{ row =>
          val oid = row.getString(0).toInt
          val lon = row.getString(1).toDouble
          val lat = row.getString(2).toDouble
          val tid = row.getString(3).toInt

          val point = G.createPoint(new Coordinate(lon, lat))
          point.setUserData(Data(oid, tid))

          (tid, point)
        }
      }

    val buffer = Source.fromFile("/home/acald013/Research/Meetings/next/figures/sample/cells.wkt")
    val reader = new WKTReader(G)
    val tree = new STRtree()
    val cells = buffer.getLines().map{ line =>
      val arr = line.split("\t")
      val pid = arr(0).toInt
      val env = reader.read(arr(1)).getEnvelopeInternal


      pid -> env
    }.toMap
    cells.foreach{ case(pid, env) =>
      tree.insert(env, pid)
    }

    debug {
      save("/tmp/edgesCells.wkt") {
        cells.map { case(id, envelope) =>
          val wkt = G.toGeometry(envelope).toText
          s"$wkt\t$id\n"
        }.toList
      }
    }

    val trajs_partitioned = trajs.mapPartitions{ rows =>
      rows.flatMap{ case(_, point) =>
        tree.query(point.getEnvelopeInternal).asScala
          .map{ x => (x.asInstanceOf[Int], point) }
      }
    }.partitionBy( SimplePartitioner(cells.size) ).map(_._2).cache

    val nTrajs = trajs_partitioned.count()
    log(s"Number of trajectories: $nTrajs")

    save("/tmp/edgesP.wkt") {
      trajs_partitioned.mapPartitionsWithIndex { (pid, points) =>
        points.map { point =>
          s"${point.toText}\t$pid\n"
        }
      }.collect
    }

    val flocksRDD = trajs_partitioned.mapPartitionsWithIndex{ (index, points) =>
      val cell = cells(index)
      cell.expandBy(S.sdist * -1.0)

      val ps = points.toList.map{ point =>
        val data = point.getUserData.asInstanceOf[Data]
        (data.t, point)
      }.groupBy(_._1).map{ case(time, points) =>
        (time, points.map( p => STPoint(p._2) ))
      }.toList.sortBy(_._1)

      val (flocks, partial) = PF_Utils.joinDisks(ps, List.empty[Disk], List.empty[Disk], cell, cell, List.empty[Disk])

      partial.foreach(_.did = index)
      partial.toIterator
    }

    val fn = flocksRDD.count()
    log(s"Reported flocks: $fn")

    val P = flocksRDD.sortBy(_.start).collect()
    val starts = P.groupBy(_.start)
    val ends   = P.groupBy(_.end).toSeq.sortBy(_._1)

    val J = ends.flatMap{ case(end, partial_end) =>
      val partial_start = try {
        starts(end + 1)
      } catch {
        case e: Exception => Array.empty[Disk]
      }

      for{
        s <- partial_start
        e <- partial_end if(s.did != e.did)
      } yield {
        (e, s)
      }
    }

    val partial_flocks = J.filter{ case(s, e) =>
      s.pidsSet.intersect(e.pidsSet).size >= S.mu
    }.map{ case(s,e) =>
      val pids = s.pidsSet.intersect(e.pidsSet)
      val start = if (e.end - s.start >= S.delta)
        e.end - S.delta + 1
      else
        s.start
      val end = e.end
      val locations = s.locations ++ e.locations

      val flock = Disk(e.center, pids.toList.sorted, start, end)
      flock.locations = locations
      flock
    }.filter{ f => (f.end - f.start) == (S.delta - 1) }.distinct

    partial_flocks.foreach{println}

    spark.close
  }
}
