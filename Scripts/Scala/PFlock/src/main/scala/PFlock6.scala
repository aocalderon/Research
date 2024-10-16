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

object PFlock6 {
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
      }.cache
    trajs.count()

    // Time partitions...
    val times_prime = (0 to S.endtime).toList
    val time_partitions = PF_Utils.cut(times_prime, params.step())

    debug {
      time_partitions.toSeq.sortBy(_._1).foreach {
        println
      }
    }

    val sample = trajs.sample(withReplacement = false, fraction = params.fraction(), seed = 42).collect()
    val envelope = PF_Utils.getEnvelope(trajs)
    envelope.expandBy(S.epsilon * 2.0)
    implicit val quadtree: StandardQuadTree[Point] = new StandardQuadTree[Point](new QuadRectangle(envelope), 0, params.capacity(), 16)
    sample.foreach { case (_, point) =>
      quadtree.insert(new QuadRectangle(point.getEnvelopeInternal), point)
    }
    quadtree.assignPartitionIds()
    quadtree.assignPartitionLineage()
    quadtree.dropElements()
    implicit val cells: Map[Int, Cell] = quadtree.getLeafZones.asScala.map { leaf =>
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

    var t0 = clocktime
    val trajs_prime = trajs.filter(_._1 <= S.endtime).mapPartitions { rows =>
      rows.flatMap { case (_, point) =>
        val tpart = time_partitions(PF_Utils.getTime(point))
        val env = point.getEnvelopeInternal
        env.expandBy(S.epsilon)
        quadtree.findZones(new QuadRectangle(env)).asScala
          .map { x => ((x.partitionId.toInt, tpart), point) }
      }
    }.cache
    implicit val cubes_ids: Map[(Int, Int), Int] = trajs_prime.map{ _._1 }.distinct().collect().sortBy(_._1).sortBy(_._2).zipWithIndex.toMap
    implicit val cubes_ids_inverse: Map[Int, (Int, Int)] = cubes_ids.map{ case(key, value) => value -> key }

    save("/tmp/cubes_ids.tsv"){
      cubes_ids.map{ case(k, v) =>
        s"$v\t${k._1}\t${k._2}\n"
      }.toList
    }

    val trajs_partitioned0 = trajs_prime.map{ case(tuple_id, point) =>
      val cube_id = cubes_ids(tuple_id)
      (cube_id, point)
    }.partitionBy(SimplePartitioner(cubes_ids.size)).map(_._2).cache
    log(s"$capa|$ncells|$sdist|$step|Part|${trajs_partitioned0.count()}")
    val tPart = (clocktime - t0) / 1e9
    logt(s"$capa|$ncells|$sdist|$step|Part|$tPart")

    val trajs_partitioned = trajs_partitioned0.filter{ p =>
      val data = p.getUserData.asInstanceOf[Data]
      data.t <= S.endtime
    }/*.filter{ p =>
      val data = p.getUserData.asInstanceOf[Data]
      val ids = Set(3191, 3962, 9435)

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

    val cids2 = Set(82,78,149,157,141,79,2941,43,95,2957)
    save("/tmp/edgesP.wkt"){
      trajs_partitioned0.mapPartitionsWithIndex{ (index, points) =>
        if(cids2.contains(index)){
          points.map{ point =>
            val wkt = point.toText
            val dat = point.getUserData.asInstanceOf[Data]

            s"$wkt\t$dat\n"
          }
        } else {
          List.empty[String].toIterator
        }
      }.collect
    }

    val nTrajs = trajs_partitioned.count()
    log(s"Number of trajectories: $nTrajs")

    log("Start.")
    t0 = clocktime
    val flocksRDD = trajs_partitioned.mapPartitionsWithIndex { (index, points) =>
      val t0 = clocktime
      val cell_id = cubes_ids_inverse(index)._1
      val cell_test = new Envelope(cells(cell_id).mbr)
      cell_test.expandBy(S.sdist * -1.0)
      val cell = if(cell_test.isNull){
        new Envelope(cells(cell_id).mbr.centre())
      } else {
        cell_test
      }
      val cell_prime = new Envelope(cells(cell_id).mbr)

      val ps = points.toList.map { point =>
        val data = point.getUserData.asInstanceOf[Data]
        (data.t, point)
      }.groupBy(_._1).map { case (time, points) =>
        (time, points.map(p => STPoint(p._2)))
      }.toList.sortBy(_._1)
      val times_prime = ps.map(_._1)
      val r = if(times_prime.isEmpty){
        val r = (List.empty[Disk],List.empty[Disk],List.empty[Disk])
        Iterator(r)
      } else {
        val time_start = times_prime.head
        val time_end = times_prime.last

        val flocks_and_partials = PF_Utils.joinDisksCachingPartials(ps, List.empty[Disk], List.empty[Disk],
          cell, cell_prime, List.empty[Disk],
          time_start, time_end, List.empty[Disk], List.empty[Disk])

        Iterator(flocks_and_partials)
      }
      val t1 = (clocktime - t0) / 1e9
      logt(s"$capa|$ncells|$sdist|$step|$index|PerCell|Safe|$t1")

      r
    }.cache
    val flocksLocal = flocksRDD.collect()
    val safes = flocksLocal.flatMap(_._1)
    val tSafe = (clocktime - t0) / 1e9
    logt(s"$capa|$ncells|$sdist|$step|Safe|$tSafe")
    log(s"$capa|$ncells|$sdist|$step|Safe|${safes.length}")

    /****
     * Spatial partitioning...
     */
    def mca(l1: String, l2: String): String = {
      val i = l1.zip(l2).map{ case(a, b) => a == b }.indexOf(false)
      l1.substring(0, i)
    }
    t0 = clocktime
    val spartialsRDD_prime = flocksRDD.mapPartitionsWithIndex{ (index, flocks) =>
      val t0 = clocktime
      val cube_id = cubes_ids_inverse(index)
      val cell_id = cube_id._1
      val time_id = cube_id._2
      val cell = cells(cell_id)
      val R = flocks.flatMap(_._2).flatMap{ partial =>
        val parents = quadtree
          .findZones( new QuadRectangle(partial.getExpandEnvelope(S.sdist + S.tolerance)) )
          .asScala
          .filter( zone => zone.partitionId != cell.cid).toList
          .map{ zone =>
            val lin = mca(zone.lineage, cell.lineage)
            partial.lineage = lin
            partial.did = index
            ((lin, time_id), partial)
          }
        parents
      }
      val t1 = (clocktime - t0) / 1e9
      logt(s"$capa|$ncells|$sdist|$step|$index|PerCell|SPartial1|$t1")

      R
    }.cache
    val cids = spartialsRDD_prime.map(_._1).distinct().collect().zipWithIndex.toMap
    val spartialsRDD = spartialsRDD_prime.map{ case(cid, partial) =>
      (cids(cid), partial)
    }
      .partitionBy(SimplePartitioner(cids.size))
      .mapPartitionsWithIndex{ (index, p_prime) =>
        val t0 = clocktime
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
        val t1 = (clocktime - t0) / 1e9
        logt(s"$capa|$ncells|$sdist|$step|$index|PerCell|SPartial2|$t1")

        R.toIterator
    }
    val spartials = spartialsRDD.collect()
    val tSPartials = (clocktime - t0) / 1e9
    logt(s"$capa|$ncells|$sdist|$step|SPartial|$tSPartials")
    log(s"$capa|$ncells|$sdist|$step|SPartial|${spartials.length}")

    /****
     * Temporal partitioning
     */
    t0 = clocktime
    val tpartialsRDD = flocksRDD.mapPartitionsWithIndex { (_, flocks) =>
      flocks.flatMap(_._3).flatMap { tpartial =>
        val envelope = tpartial.getExpandEnvelope(S.sdist + S.tolerance)
        quadtree.findZones(new QuadRectangle(envelope)).asScala.map { zone =>
          (zone.partitionId.toInt, tpartial)
        }
      }
    }.partitionBy(SimplePartitioner(ncells)).mapPartitionsWithIndex{ (index, prime) =>
      val t0 = clocktime
      val cell = cells(index)
      val tpartial = prime.map(_._2).toList
      val P = tpartial.sortBy(_.start).groupBy(_.start)
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
      val R = PF_Utils.processPartials(List.empty[Disk], times, partials, List.empty[Disk]).filter{ f => cell.contains(f) }
      val t1 = (clocktime - t0) / 1e9
      logt(s"$capa|$ncells|$sdist|$step|$index|PerCell|TPartial|$t1")

      R.toIterator
    }.cache
    val tpartials = tpartialsRDD.collect()
    val tTPartials = (clocktime - t0) / 1e9
    logt(s"$capa|$ncells|$sdist|$step|TPartial|$tTPartials")
    log(s"$capa|$ncells|$sdist|$step|TPartial|${tpartials.length}")

    t0 = clocktime
    val N = PF_Utils.parPrune(safes.toList ++ spartials.toList ++ tpartials.toList)
    val tParPrune = (clocktime - t0) / 1e9
    logt(s"$capa|$ncells|$sdist|$step|parPrune|$tParPrune")
    log(s"$capa|$ncells|$sdist|$step|parPrune|${N.size}")

    logt(s"$capa|$ncells|$sdist|$step|Total|${tSafe + tSPartials + tTPartials + tParPrune}")
    log(s"$capa|$ncells|$sdist|$step|Total|${N.size}")

    if(S.debug) {
      save("/tmp/pflockd6.tsv") {
        N.map { n =>
          val s = n.start
          val e = n.end
          val m = n.pidsText
          s"$s\t$e\t$m\n"
        }.sorted
      }
    }

    spark.close
  }
}
