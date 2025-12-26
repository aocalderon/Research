package puj

import edu.ucr.dblab.pflock.sedona.quadtree.{QuadRectangle, StandardQuadTree}
import edu.ucr.dblab.pflock.sedona.quadtree.Quadtree.Cell

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.scala.Logging

import org.locationtech.jts.geom._
import org.locationtech.jts.index.strtree.STRtree


import scala.collection.JavaConverters.asScalaBufferConverter

import puj.Utils._
import puj.bfe._
import puj.psi._

object PFlocks extends Logging {
  
  def main(args: Array[String]): Unit = {
    logger.info("Starting PFlocks computation")

    /*************************************************************************
     * Settings...
     *************************************************************************/    
    implicit val params: Params = new Params(args)  // Reading parameters...

    implicit val spark: SparkSession = SparkSession.builder()  // Starting Spark...
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .master(params.master())
      .appName("PFlock").getOrCreate()

    implicit var S = Settings(  // Defining settings...
      dataset = params.input(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = "PSI",
      scapacity = params.scapacity(),
      tcapacity = params.tcapacity(),
      sdist = params.sdist(),
      step = params.step(),
      tolerance = params.tolerance(),
      endtime = params.endtime(),
      fraction = params.fraction(),
      debug = params.debug()
    )

    implicit val G: GeometryFactory = new GeometryFactory(new PrecisionModel(S.scale)) // Setting precision model and geofactory...

    S.printer

    /*************************************************************************
     * Reading data...
     *************************************************************************/    
    val trajs = spark.read // Reading trajectories...
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

    /*************************************************************************
     * Spatiotemporal partitioning...
     *************************************************************************/    
    val t0 = clocktime // Starting timer...
    val times_prime = (0 to S.endtime).toList
    // Temporal partitions...
    val time_partitions = PF_Utils.cut(times_prime, S.step) 

    val sample = trajs.sample(withReplacement = false, fraction = S.fraction, seed = 42).collect()
    val envelope = PF_Utils.getEnvelope(trajs)
    envelope.expandBy(S.epsilon * 2.0)
    // Spatial partitions...
    implicit val quadtree: StandardQuadTree[Point] = new StandardQuadTree[Point](new QuadRectangle(envelope), 0, S.scapacity, 16) 
    sample.foreach { case (_, point) =>
      quadtree.insert(new QuadRectangle(point.getEnvelopeInternal), point)
    }
    quadtree.assignPartitionIds()
    quadtree.assignPartitionLineage()
    quadtree.dropElements()
    
    // Getting quadtree cells...
    implicit val cells: Map[Int, Cell] = quadtree.getLeafZones.asScala.map { leaf => 
      val envelope = leaf.getEnvelope
      val id = leaf.partitionId.toInt

      id -> Cell(id, envelope, leaf.lineage)
    }.toMap

    // Assigning points to spatiotemporal partitions...
    val trajs_prime = trajs.filter(_._1 <= S.endtime).mapPartitions { rows => 
      rows.flatMap { case (_, point) =>
        val tpart = time_partitions(PF_Utils.getTime(point))
        val env = point.getEnvelopeInternal
        env.expandBy(S.epsilon)
        quadtree.findZones(new QuadRectangle(env)).asScala.map{ x => 
          ((x.partitionId.toInt, tpart), point) 
        }
      }
    }.cache
    
    // Assigning unique IDs to spatiotemporal partitions...
    implicit val cubes_ids: Map[(Int, Int), Int] = trajs_prime.map{ _._1 }.distinct().collect().sortBy(_._1).sortBy(_._2).zipWithIndex.toMap
    // Inverse map for spatiotemporal partitions...
    implicit val cubes_ids_inverse: Map[Int, (Int, Int)] = cubes_ids.map{ case(key, value) => value -> key }

    // Repartitioning trajectories according to spatiotemporal partitions...
    val trajs_partitioned = trajs_prime.map{ case(tuple_id, point) =>
      val cube_id = cubes_ids(tuple_id)
      (cube_id, point)
    }.partitionBy(SimplePartitioner(cubes_ids.size)).map(_._2).filter{ p =>
      val data = p.getUserData.asInstanceOf[Data]
      data.tid <= S.endtime
    }
    val nTrajs = trajs_partitioned.count()
    val tTrajs = (clocktime - t0) / 1e9

    logger.info(s"TIME|STPart|$tTrajs")
    logger.info(s"INFO|STPart|$nTrajs")

    // Debugging info...
    debug{
      save("/tmp/edgesCells.wkt") {
        cells.map { case(id, cell) =>
          val wkt = G.toGeometry(cell.envelope).toText
          s"$wkt\tL${cell.lineage}\t$id\n"
        }.toList
      }
      save("/tmp/cubes_ids.tsv"){
        cubes_ids.map{ case(k, v) =>
          s"$v\t${k._1}\t${k._2}\n"
        }.toList
      }
    }

    /*************************************************************************
     * Safe Flocks
     *************************************************************************/    
    val ( (flocksRDD, nFlocksRDD), tFlocksRDD ) = timer {
      // Computing flocks in each spatiotemporal partition...
      val flocksRDD = trajs_partitioned.mapPartitionsWithIndex { (index, points) =>
        val t0 = clocktime
        val cell_id = cubes_ids_inverse(index)._1
        val cell_test = new Envelope(cells(cell_id).envelope)
        cell_test.expandBy(S.sdist * -1.0)
        val cell = if(cell_test.isNull){
          new Envelope(cells(cell_id).envelope.centre())
        } else {
          cell_test
        }
        val cell_prime = new Envelope(cells(cell_id).envelope)
        val ps = points.toList.map { point =>
          val data = point.getUserData.asInstanceOf[Data]
          (data.tid, point)
        }.groupBy(_._1).map { case (time, points) =>
          (time, points.map(p => STPoint(p._2)))
        }.toList.sortBy(_._1)
        val times_prime = ps.map(_._1)
        val r = if(times_prime.isEmpty){
          val r = (List.empty[Disk], List.empty[Disk], List.empty[Disk])
          Iterator(r)
        } else {
          val time_start = times_prime.head
          val time_end = times_prime.last

          val flocks_and_partials = PF_Utils.joinDisksCachingPartials(
            ps, 
            List.empty[Disk], 
            List.empty[Disk],
            cell, 
            cell_prime, 
            List.empty[Disk],
            time_start, 
            time_end, 
            List.empty[Disk], 
            List.empty[Disk]
          )

          Iterator(flocks_and_partials)
        }
        val t1 = (clocktime - t0) / 1e9
        debug{
          logger.info(s"${S.scapacity}|${cells.size}|${S.sdist}|${S.step}|$index|PerCell|Safe|$t1")
        }

        r
      }.cache
      val nFlocksRDD = flocksRDD.count()
      (flocksRDD, nFlocksRDD)
    }
    val safes = flocksRDD.collect().flatMap(_._1)
    logger.info(s"TIME|Safe|$tFlocksRDD")
    logger.info(s"INFO|Safe|$nFlocksRDD")


    /*************************************************************************
     * Spatial partitioning
     *************************************************************************/
    val (spartials, tSpartials) = timer {
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
            .filter( zone => zone.partitionId != cell.id).toList
            .map{ zone =>
              val lin = PF_Utils.mca(zone.lineage, cell.lineage)
              partial.lineage = lin
              partial.did = index
              ((lin, time_id), partial)
            }
          parents
        }
        val t1 = (clocktime - t0) / 1e9
        debug{
          logger.info(s"${S.scapacity}|${cells.size}|${S.sdist}|${S.step}|$index|PerCell|SPartial1|$t1")
        }

        R
      }.cache
      val cids = spartialsRDD_prime.map(_._1).distinct().collect().zipWithIndex.toMap
      val spartialsRDD = spartialsRDD_prime.map{ case(cid, partial) =>
        (cids(cid), partial)
      }.partitionBy(SimplePartitioner(cids.size)).mapPartitionsWithIndex{ (index, p_prime) =>
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
          val times = (0 to S.endtime).toList
          val R = PF_Utils.processPartials(List.empty[Disk], times, partials, List.empty[Disk])
          val t1 = (clocktime - t0) / 1e9
          debug{
            logger.info(s"${S.scapacity}|${cells.size}|${S.sdist}|${S.step}|$index|PerCell|SPartial2|$t1")
          }

          R.toIterator
      }
      spartialsRDD.collect()
    }
    logger.info(s"TIME|SPartial|$tSpartials")
    logger.info(s"INFO|SPartial|${spartials.length}")

    /*************************************************************************
     * Temporal partitioning
     *************************************************************************/
    val (tpartials, tTpartials) = timer {
      val tpartialsRDD = flocksRDD.mapPartitionsWithIndex { (_, flocks) =>
        flocks.flatMap(_._3).flatMap { tpartial =>
          val envelope = tpartial.getExpandEnvelope(S.sdist + S.tolerance)
          quadtree.findZones(new QuadRectangle(envelope)).asScala.map { zone =>
            (zone.partitionId.toInt, tpartial)
          }
        }
      }.partitionBy(SimplePartitioner(cells.size)).mapPartitionsWithIndex{ (index, prime) =>
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
        val times = (0 to S.endtime).toList
        val R = PF_Utils.processPartials(List.empty[Disk], times, partials, List.empty[Disk]).filter{ f => cell.contains(f) }
        val t1 = (clocktime - t0) / 1e9
        debug{
          logger.info(s"${S.scapacity}|${cells.size}|${S.sdist}|${S.step}|$index|PerCell|TPartial|$t1")
        }

        R.toIterator
      }.cache
      tpartialsRDD.collect()
    }
    logger.info(s"TIME|TPartial|$tTpartials")
    logger.info(s"INFO|TPartial|${tpartials.length}")

    val (flocks, tPrune) = timer {
      PF_Utils.parPrune(safes.toList ++ spartials.toList ++ tpartials.toList)
    }
    logger.info(s"TIME|Flocks|$tPrune")
    logger.info(s"INFO|Flocks|${flocks.size}")

    logger.info(s"TIME|Total|${tFlocksRDD + tSpartials + tTpartials + tPrune}")
    logger.info(s"INFO|Total|${flocks.size}")

    debug{
      save("/tmp/flocks.tsv") {
        flocks.map { n =>
          val s = n.start
          val e = n.end
          val m = n.pidsText
          s"$s\t$e\t$m\n"
        }.sorted
      }
    }

    spark.close
    logger.info("PFlocks computation ended.")
  }
}
