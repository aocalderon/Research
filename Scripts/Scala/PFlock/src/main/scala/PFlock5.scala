package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.MF_Utils.SimplePartitioner
import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.sedona.quadtree.{QuadRectangle, StandardQuadTree}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom._
import org.locationtech.jts.index.strtree.STRtree
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters.asScalaBufferConverter

object PFlock5 {
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
      endtime = params.endtime(),
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
    envelope.expandBy(S.epsilon * 2.0)
    val quadtree = new StandardQuadTree[Point](new QuadRectangle(envelope), 0, params.capacity(), 16)
    sample.foreach { case (_, point) =>
      quadtree.insert(new QuadRectangle(point.getEnvelopeInternal), point)
    }
    quadtree.assignPartitionIds()
    quadtree.assignPartitionLineage()
    quadtree.dropElements()
    val cells = quadtree.getLeafZones.asScala.map { leaf =>
      val envelope = leaf.getEnvelope
      val id = leaf.partitionId.toInt

      id -> Cell(envelope, id, leaf.lineage)
    }.toMap

    save("/tmp/edgesCells.wkt") {
      cells.map { case(id, cell) =>
        val wkt = G.toGeometry(cell.mbr).toText
        s"$wkt\tL${cell.lineage}\t$id\n"
      }.toList
    }
    val ncells = cells.size
    val sdist = params.sdist()
    val step  = params.step()
    val capa  = params.capacity()

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
      data.t <= S.endtime
    }/*.filter{ p =>
      val data = p.getUserData.asInstanceOf[Data]
      val ids = Set(838, 1619, 7124, 14590, 19287)

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

    log("Start.")
    var t0 = clocktime
    val flocksRDD = trajs_partitioned.mapPartitionsWithIndex { (index, points) =>
      val cell_test = new Envelope(cells(index).mbr)
      cell_test.expandBy(S.sdist * -1.0)
      val cell = if(cell_test.isNull){
        new Envelope(cells(index).mbr.centre())
      } else {
        cell_test
      }
      val cell_prime = new Envelope(cells(index).mbr)

      val ps = points.toList.map { point =>
        val data = point.getUserData.asInstanceOf[Data]
        (data.t, point)
      }.groupBy(_._1).map { case (time, points) =>
        (time, points.map(p => STPoint(p._2)))
      }.toList.sortBy(_._1)

      val (flocks, partial) = PF_Utils.joinDisks2(ps, List.empty[Disk], List.empty[Disk], cell, cell_prime, List.empty[Disk])

      partial.foreach{ f =>
        f.did = index
        f.dids = List(index)
      }

      (flocks ++ partial).toIterator
    }.cache
    val flocksLocal = flocksRDD.collect()
    val safes = flocksLocal.filter(_.did == -1)
    val tSafe = (clocktime - t0) / 1e9
    logt(s"$capa|$ncells|$sdist|$step|Safe|$tSafe")
    log(s"$capa|$ncells|$sdist|$step|Safe|${safes.length}")

    def mca(l1: String, l2: String): String = {
      val i = l1.zip(l2).map{ case(a, b) => a == b }.indexOf(false)
      l1.substring(0, i)
    }
    t0 = clocktime
    val partialsRDD = flocksRDD.mapPartitionsWithIndex{ (index, flocks) =>
      val cell = cells(index)
      flocks.filter(_.did != -1).flatMap{ partial =>
        val parents = quadtree
          .findZones( new QuadRectangle(partial.getExpandEnvelope(S.sdist + S.tolerance)) )
          .asScala
          .filter( zone => zone.partitionId != cell.cid).toList
          .map{ zone =>
            val lin = mca(zone.lineage, cell.lineage)
            partial.lineage = lin
            partial.did = index
            (lin, partial)
          }
        parents
      }
    }.cache
    val lins = partialsRDD.map(_._1).distinct().collect().zipWithIndex.toMap
    val R = partialsRDD.map{ case(lin, partial) =>
      (lins(lin), partial)
    }.partitionBy(SimplePartitioner(lins.size)).cache
    val Q = R.mapPartitionsWithIndex{ (_, p_prime) =>
      val pp = p_prime.map{ case(_, partial) => partial }.toList
      val P = pp.sortBy(_.start).groupBy(_.start)
      val partials = collection.mutable.HashMap[Int, (List[Disk], STRtree)]()
      P.toSeq.map{ case(time, candidates) =>
        val tree = new STRtree()
        candidates.foreach{ candidate =>
          tree.insert(new Envelope(candidate.locations.head), candidate)
        }
        partials(time) = (candidates, tree)
      }
      //val times = partials.keys.toList.sorted
      val times = (0 to S.endtime).toList
      val R = PF_Utils.processPartials(List.empty[Disk], times, partials, List.empty[Disk])

      R.toIterator
    }.cache
    val T = Q.collect().toList

    var q0 = clocktime
    val (r1, r2) = T.partition{ partial =>
      val r = quadtree
        .findZones( new QuadRectangle(partial.getExpandEnvelope(S.sdist + S.r + S.tolerance)) )

      r.size > 2
    }
    val RR = r2 ++ PF_Utils.pruneByArchery(r1)
    val q1 = (clocktime - q0) / 1e9
    logt(s"$capa|$ncells|$sdist|$step|prune1|$q1")

    q0 = clocktime
    val SS = safes.filter{ flock =>
      val r = quadtree
        .findZones( new QuadRectangle(flock.getExpandEnvelope(S.sdist + S.r + S.tolerance)) )

      r.size > 1
    }
    val FF = PF_Utils.pruneByLocation(RR, SS.toList)
    val q2 = (clocktime - q0) / 1e9
    logt(s"$capa|$ncells|$sdist|$step|prune2|$q2")

    val tPartial = (clocktime - t0) / 1e9
    logt(s"$capa|$ncells|$sdist|$step|MCA|${tPartial-q1-q2}")
    logt(s"$capa|$ncells|$sdist|$step|Partial|$tPartial")
    val npartials = flocksLocal.filter(_.did != -1).size
    log(s"$capa|$ncells|$sdist|$step|npartials|$npartials")
    log(s"$capa|$ncells|$sdist|$step|Partials|${FF.size}")

    logt(s"$capa|$ncells|$sdist|$step|Total|${tSafe + tPartial}")

    save("/home/acald013/tmp/flocksd2.tsv") {
      (FF ++ safes).map{ f =>
        val s = f.start
        val e = f.end
        val p = f.pidsText

        s"$s\t$e\t$p\n"
      }.sorted
    }

    spark.close
  }
}
