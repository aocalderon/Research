package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.MF_Utils.SimplePartitioner
import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.sedona.quadtree.{QuadRectangle, StandardQuadTree}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom._
import org.slf4j.{Logger, LoggerFactory}

import scala.annotation.tailrec
import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.mutable

object PFlock2 {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params: BFEParams = new BFEParams(args)

    implicit val spark: SparkSession = SparkSession.builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
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

    /** **************************************************************************** */
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

          val point = G.createPoint(new Coordinate(lon, lat))
          point.setUserData(Data(oid, tid))

          (tid, point)
        }
      }

    val sample = trajs.sample(withReplacement = false, fraction = params.fraction(), seed = 42).collect()
    val envelope = PF_Utils.getEnvelope(trajs)
    envelope.expandBy(S.epsilon)
    val quadtree = new StandardQuadTree[Point](new QuadRectangle(envelope), 0, params.capacity(), 16)
    sample.foreach { case (_, point) =>
      quadtree.insert(new QuadRectangle(point.getEnvelopeInternal), point)
    }
    quadtree.assignPartitionIds()
    quadtree.assignPartitionLineage()
    quadtree.dropElements()
    val cells = quadtree.getLeafZones.asScala.map { leaf =>
      val envelope = leaf.getEnvelope
      val id = leaf.partitionId

      id -> envelope
    }.toMap

    save("/tmp/edgesCells.wkt") {
      cells.map { case (id, envelope) =>
        val wkt = G.toGeometry(envelope).toText
        s"$wkt\t$id\n"
      }.toList
    }

    val trajs_partitioned0 = trajs.mapPartitions { rows =>
      rows.flatMap { case (_, point) =>
        val env = point.getEnvelopeInternal
        env.expandBy(S.epsilon)
        quadtree.findZones(new QuadRectangle(env)).asScala
          .map { x => (x.partitionId, point) }
      }
    }.partitionBy(SimplePartitioner(quadtree.getLeafZones.size())).map(_._2).cache


    val trajs_partitioned = trajs_partitioned0.filter{ p =>
      val data = p.getUserData.asInstanceOf[Data]
      data.t <= 10
    }
 /*.filter{ p =>
      val data = p.getUserData.asInstanceOf[Data]
      val ids = Set(10153, 17624, 18995)
      ids.contains(data.id)
 }
    save("/home/acald013/tmp/demo.tsv") {
      trajs_partitioned.map { p =>
        val data = p.getUserData.asInstanceOf[Data]
        val i = data.id
        val x = p.getX
        val y = p.getY
        val t = data.t
        s"$i\t$x\t$y\t$t\n"
      }.collect()
    }*/

    val nTrajs = trajs_partitioned.count()
    log(s"Number of trajectories: $nTrajs")

    debug {
      save("/tmp/edgesS.wkt") {
        trajs_partitioned.sample(withReplacement = false, 0.1, 42).mapPartitionsWithIndex { (i, points) =>
          points.map { point =>
            val wkt = point.toText
            s"$wkt\t$i\n"
          }
        }.collect()
      }
    }

    val flocksRDD = trajs_partitioned.mapPartitionsWithIndex { (index, points) =>
      val cell_test = new Envelope(cells(index))
      cell_test.expandBy(S.sdist * -1.0)
      val cell = if(cell_test.isNull){
        new Envelope(cells(index).centre())
      } else {
        cell_test
      }
      val cell_prime = new Envelope(cells(index))

      val ps = points.toList.map { point =>
        val data = point.getUserData.asInstanceOf[Data]
        (data.t, point)
      }.groupBy(_._1).map { case (time, points) =>
        (time, points.map(p => STPoint(p._2)))
      }.toList.sortBy(_._1)

      val (flocks, partial) = PF_Utils.joinDisks(ps, List.empty[Disk], List.empty[Disk], cell, cell_prime, List.empty[Disk])

      partial.foreach(_.did = index)
      (flocks ++ partial).toIterator
    }.cache

    val safes = flocksRDD.filter(_.did == -1)
    log(s"Reported flocks: ${safes.count}")

    val P_prime = flocksRDD.filter(_.did != -1).sortBy(_.start).collect()
    log(s"For processing ${P_prime.size}")
    val P = P_prime.groupBy(_.start)

    val partials = collection.mutable.HashMap[Int, List[Disk]]()
    P.toSeq.map{ case(time, candidates) =>
      partials(time) = candidates.toList
    }

    //val times = partials.keys.toList.sorted
    val times = (0 to 10).toList

    @tailrec
    def processPartials(F: List[Disk], times: List[Int], partials: mutable.HashMap[Int, List[Disk]], R: List[Disk]): List[Disk] = {
      times match {
        case time::tail =>
          val (f_prime, r_prime) = PF_Utils.funPartial(F, time, partials, R)
          processPartials(f_prime, tail, partials, r_prime)
        case Nil => R
      }
    }

    val R = processPartials(List.empty[Disk], times, partials, List.empty[Disk])

    log(s"Partial flocks: ${R.size}")
    save("/home/acald013/tmp/flocks_border.tsv") {
      R.map{ f =>
        val s = f.start
        val e = f.end
        val p = f.pidsText

        s"$s\t$e\t$p\n"
      }.sorted
    }

    val FF = PF_Utils.prune2(R, safes.collect().toList, List.empty[Disk])

    save("/home/acald013/tmp/flocksd.tsv") {
      FF.map{ f =>
        val s = f.start
        val e = f.end
        val p = f.pidsText

        s"$s\t$e\t$p\n"
      }.sorted
    }
    /*
    val E = PF_Utils.funPartial(List.empty[Disk], 0, partials)
    val J = PF_Utils.funPartial(E, 1,  partials)
    val K = PF_Utils.funPartial(J, 2,  partials)
    val L = PF_Utils.funPartial(K, 3,  partials)
    val O = PF_Utils.funPartial(L, 4,  partials)
    val Q = PF_Utils.funPartial(O, 5,  partials)
    val U = PF_Utils.funPartial(Q, 6,  partials)
    val V = PF_Utils.funPartial(U, 7,  partials)
    val W = PF_Utils.funPartial(V, 8,  partials)
    val X = PF_Utils.funPartial(W, 9,  partials)
    val Z = PF_Utils.funPartial(X, 10,  partials)*/

    spark.close
  }
}
