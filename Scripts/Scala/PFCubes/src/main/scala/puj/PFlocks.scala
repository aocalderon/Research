package puj

import edu.ucr.dblab.pflock.sedona.quadtree.{ QuadRectangle, StandardQuadTree }

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.scala.Logging

import org.locationtech.jts.geom._
import org.locationtech.jts.index.strtree.STRtree

import scala.collection.JavaConverters.asScalaBufferConverter

import puj.Utils._
import puj.partitioning._
import puj.bfe._
import puj.psi._

object PFlocks extends Logging {

  def main(args: Array[String]): Unit = {

    /************************************************************************* 
     * Settings...
     */
    implicit var S: Settings        = Setup.getSettings(args) // Initializing settings...
    implicit val G: GeometryFactory = S.geofactory            // Initializing geometry factory...

    // Starting Spark...
    implicit val spark: SparkSession = SparkSession
      .builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .master(S.master)
      .appName("PFlock")
      .getOrCreate()

    S.appId = spark.sparkContext.applicationId
    logger.info(s"${S.appId}|START|Starting PFlocks computation")
    S.printer

    /*************************************************************************
     * Data reading...
     */
    val trajs = spark.read // Reading trajectories...
      .option("header", value = false)
      .option("delimiter", "\t")
      .csv(S.dataset)
      .rdd
      .mapPartitions {
        rows =>
          rows.map {
            row =>
              val oid = row.getString(0).toInt
              val lon = row.getString(1).toDouble
              val lat = row.getString(2).toDouble
              val tid = row.getString(3).toInt

              val point = G.createPoint(new Coordinate(lon, lat))
              point.setUserData(Data(oid, tid))

              point
          }
      }
      .cache
    trajs.count()

    /************************************************************************* 
     * Spatio-temporal partitioning...
     */

    implicit val ((trajs_partitioned, cubes, quadtree), tTrajs) = timer {
      if (S.partitioner == "Fixed") {
        logger.info(s"${S.appId}|INFO|Partitioner|${S.partitioner}")
        CubePartitioner.getFixedIntervalCubes(trajs)
      } else {
        logger.info(s"${S.appId}|INFO|Partitioner|${S.partitioner}")
        CubePartitioner.getDynamicIntervalCubes(trajs)
      }
    }
    val nTrajs                                                  = trajs_partitioned.count()
    logger.info(s"${S.appId}|TIME|STPart|$tTrajs")
    logger.info(s"${S.appId}|INFO|STPart|$nTrajs")

    logger.info(s"${S.appId}|INFO|Cubes|${cubes.size}")

    // Debugging info...
    debug {
      save("/tmp/Cubes.wkt") {
        cubes.values.map {
          cube =>
            s"${cube.wkt}\n"
        }.toList
      }
    }
    experiments {
      save(s"${S.output}Cubes_${S.appId}.wkt") {
        cubes.values.map {
          cube =>
            s"${cube.wkt}\n"
        }.toList
      }
      trajs_partitioned.mapPartitionsWithIndex {
        (index, points) =>
          save(s"${S.output}${S.appId}_${index}.tsv") {
            points.map {
              point =>
                val data = point.getUserData.asInstanceOf[Data]
                val oid  = data.oid
                val lon  = point.getX
                val lat  = point.getY
                val tid  = data.tid
                s"$oid\t$lon\t$lat\t$tid\n"
            }.toList
          }
          points
      }.count()
    }

    /************************************************************************* 
     * Safe flocks finding...
     */
    val (flocksRDD, tFlocksRDD) = timer {
      // Computing flocks in each spatiotemporal partition...
      val flocksRDD  = trajs_partitioned.mapPartitionsWithIndex {
        (index, points) =>
          val t0          = clocktime
          val originalEnv = cubes(index).cell.envelope
          val cell_test   = new Envelope(originalEnv)
          cell_test.expandBy(S.sdist * -1.0)
          val cell        = if (cell_test.isNull) {
            new Envelope(originalEnv.centre())
          } else {
            cell_test
          }
          val cell_prime  = new Envelope(originalEnv)
          val ps          = points.toList
            .map {
              point =>
                val data = point.getUserData.asInstanceOf[Data]
                (data.tid, point)
            }
            .groupBy(_._1)
            .map {
              case (time, points) =>
                (time, points.map(p => STPoint(p._2)))
            }
            .toList
            .sortBy(_._1)
          val times_prime = ps.map(_._1)
          val r           = if (times_prime.isEmpty) {
            val r = (List.empty[Disk], List.empty[Disk], List.empty[Disk])
            Iterator(r)
          } else {
            val time_start = times_prime.head
            val time_end   = times_prime.last

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
          val tSafe       = (clocktime - t0) / 1e9
          experiments {
            logger.info(s"${S.appId}|TIME|PER_CELL|Safe|$index|$tSafe")
          }

          r
      }.cache
      val nFlocksRDD = flocksRDD.count()
      debug {
        logger.info { s"${S.appId}|INFO|nFlocksRDD|$nFlocksRDD" }
      }
      flocksRDD
    }

    val safes  = flocksRDD.collect().flatMap(_._1)
    val nSafes = safes.length
    logger.info(s"${S.appId}|TIME|Safe|$tFlocksRDD")
    logger.info(s"${S.appId}|INFO|Safe|$nSafes")

    /************************************************************************* 
     * Spatial partial finding...
     */
    val (spartials, tSpartials) = timer {
      val spartialsRDD_prime = flocksRDD.mapPartitionsWithIndex {
        (index, flocks) =>
          val t0         = clocktime
          val cube       = cubes(index)
          val cell_id    = cube.cell.id
          val time_id    = cube.interval.index
          val cell       = cube.cell
          val R          = flocks.flatMap(_._2).flatMap {
            partial =>
              val parents = quadtree
                .findZones(new QuadRectangle(partial.getExpandEnvelope(S.sdist + S.tolerance)))
                .asScala
                .filter(zone => zone.partitionId != cell.id)
                .toList
                .map {
                  zone =>
                    val lin = PF_Utils.mca(zone.lineage, cell.lineage)
                    partial.lineage = lin
                    partial.did = index
                    ((lin, time_id), partial)
                }
              parents
          }
          val tSpartial1 = (clocktime - t0) / 1e9
          experiments {
            logger.info(s"${S.appId}|TIME|PER_CELL|SPartial1|$index|$tSpartial1")
          }

          R
      }.cache
      val nSpartialRDD_prime = spartialsRDD_prime.count()

      debug {
        logger.info { s"INFO|nSpartialRDD_Prime|$nSpartialRDD_prime" }
      }

      val cids  = spartialsRDD_prime.map(_._1).distinct().collect().zipWithIndex.toMap
      val nCids = cids.size

      debug {
        logger.info { s"INFO|CIDS|$nCids" }
      }

      val spartialsRDD = spartialsRDD_prime
        .map {
          case (cid, partial) =>
            (cids(cid), partial)
        }
        .partitionBy(SimplePartitioner(cids.size))
        .mapPartitionsWithIndex {
          (index, p_prime) =>
            val t0         = clocktime
            val pp         = p_prime.map { case (_, partial) => partial }.toList
            val P          = pp.sortBy(_.start).groupBy(_.start)
            val partials   = collection.mutable.HashMap[Int, (List[Disk], STRtree)]()
            P.toSeq.map {
              case (time, candidates) =>
                val tree = new STRtree()
                candidates.foreach {
                  candidate =>
                    tree.insert(new Envelope(candidate.locations.head), candidate)
                }
                partials(time) = (candidates, tree)
            }
            val times      = (0 to S.endtime).toList
            val R          = PF_Utils.processPartials(List.empty[Disk], times, partials, List.empty[Disk])
            val tSpartial2 = (clocktime - t0) / 1e9
            experiments {
              logger.info(s"${S.appId}|TIME|PER_CELL|SPartial2|$index|$tSpartial2")
            }

            R.toIterator
        }
      spartialsRDD.collect()
    }
    logger.info(s"${S.appId}|TIME|SPartial|$tSpartials")
    logger.info(s"${S.appId}|INFO|SPartial|${spartials.length}")

    /************************************************************************* 
     * Temporal partial finding...
     */
    val ncells                  = quadtree.getLeafZones.size()
    val (tpartials, tTpartials) = timer {
      val tpartialsRDD = flocksRDD
        .mapPartitionsWithIndex {
          (_, flocks) =>
            flocks.flatMap(_._3).flatMap {
              tpartial =>
                val envelope = tpartial.getExpandEnvelope(S.sdist + S.tolerance)
                quadtree.findZones(new QuadRectangle(envelope)).asScala.map {
                  zone =>
                    (zone.partitionId.toInt, tpartial)
                }
            }
        }
        .partitionBy(SimplePartitioner(ncells))
        .mapPartitionsWithIndex {
          (index, prime) =>
            val t0        = clocktime
            val cell      = cubes(index).cell
            val tpartial  = prime.map(_._2).toList
            val P         = tpartial.sortBy(_.start).groupBy(_.start)
            val partials  = collection.mutable.HashMap[Int, (List[Disk], STRtree)]()
            P.toSeq.map {
              case (time, candidates) =>
                val tree = new STRtree()
                candidates.foreach {
                  candidate =>
                    tree.insert(new Envelope(candidate.locations.head), candidate)
                }
                partials(time) = (candidates, tree)
            }
            val times     = (0 to S.endtime).toList
            val R         = PF_Utils.processPartials(List.empty[Disk], times, partials, List.empty[Disk]).filter {
              f => cell.contains(f)
            }
            val tTpartial = (clocktime - t0) / 1e9
            experiments {
              logger.info(s"${S.appId}|TIME|PER_CELL|TPartial|$index|$tTpartial")
            }

            R.toIterator
        }
        .cache
      tpartialsRDD.collect()
    }
    logger.info(s"${S.appId}|TIME|TPartial|$tTpartials")
    logger.info(s"${S.appId}|INFO|TPartial|${tpartials.length}")

    /*********************************************************************** 
     * Duplicate prunning...
     */
    val (flocks, tPrune) = timer {
      val allFlocks = (safes ++ spartials ++ tpartials).toList
      debug {
        logger.info(s"INFO|BeforePrune|${allFlocks.length}")
      }
      PF_Utils.parPrune(allFlocks)
    }
    logger.info(s"${S.appId}|TIME|Flocks|$tPrune")
    logger.info(s"${S.appId}|INFO|Flocks|${flocks.size}")

    logger.info(s"${S.appId}|TIME|Total|${tFlocksRDD + tSpartials + tTpartials + tPrune}")
    logger.info(s"${S.appId}|INFO|Total|${flocks.size}")

    debug {
      save("/tmp/flocks.tsv") {
        flocks.map {
          n =>
            val s = n.start
            val e = n.end
            val m = n.pidsText
            s"$s\t$e\t$m\n"
        }.sorted
      }
    }

    spark.close
    logger.info(s"${S.appId}|END|PFlocks computation ended.")
  }
}
