package puj.partitioning

import org.locationtech.jts.geom.{ GeometryFactory, Point, Envelope }

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.logging.log4j.scala.Logging

import edu.ucr.dblab.pflock.sedona.quadtree.{ StandardQuadTree, QuadRectangle }

import scala.collection.JavaConverters._

import puj.Settings
import puj.PF_Utils
import puj.Utils.{ SimplePartitioner, Data, debug, save }

case class Cube(id: Int, cell: Cell, interval: Interval, st_index: Int) {

  def wkt: String = {
    val wkt = cell.wkt
    val sid = cell.id
    val tid = interval.index
    val beg = interval.begin
    val dur = interval.duration

    s"$wkt\t$id\t$sid\t$tid\t$beg\t$dur"
  }

  def toText(field: String = ""): String = {
    val wkt = cell.wkt
    val beg = interval.begin
    val dur = interval.duration

    if (field == "")
      s"$wkt\t$id\t$beg\t$dur\n"
    else
      s"$wkt\t$id\t$beg\t$dur\t$field\n"
  }
}

object CubePartitioner extends Logging {

  /** Partitions trajectory points into spatio-temporal cubes based on a quadtree for spatial partitioning and fixed
    * time intervals for temporal partitioning.
    *
    * @param trajs
    *   An RDD of trajectory points, each represented as a tuple of (time instant, Point).
    * @param S
    *   Implicit settings containing parameters for partitioning.
    * @param G
    *   Implicit geometry factory for creating geometric objects.
    * @return
    *   A tuple containing:
    *   - An RDD of partitioned trajectory points.
    *   - A map of cube IDs to Cube objects representing the spatio-temporal partitions.
    */
  def getFixedIntervalCubes(trajs: RDD[Point])(implicit
    S: Settings
  ): (RDD[Point], Map[Int, Cube], StandardQuadTree[Point]) = {

    // Building quadtree...
    debug {
      logger.info(s"Running Fixed Interval Cube Partitioner...")
    }
    implicit var G: GeometryFactory = S.geofactory
    val sample                      = trajs.sample(withReplacement = false, fraction = S.fraction, seed = 42).collect()
    val universe                    = PF_Utils.getEnvelope(trajs)
    universe.expandBy(S.epsilon * 2.0)
    val quadtree: StandardQuadTree[Point] =
      new StandardQuadTree[Point](new QuadRectangle(universe), 0, S.scapacity, 16)
    sample.foreach {
      point =>
        quadtree.insert(new QuadRectangle(point.getEnvelopeInternal), point)
    }
    quadtree.assignPartitionIds()
    quadtree.assignPartitionLineage()
    quadtree.dropElements()

    debug {
      logger.info(s"Quadtree done! ${quadtree.getLeafZones.size()} leaf zones created.")
    }

    // Getting cells...
    implicit val cells: Map[Int, Cell] = quadtree.getLeafZones.asScala.map {
      leaf =>
        val envelope = leaf.getEnvelope
        val id       = leaf.partitionId.toInt

        id -> Cell(id, envelope, leaf.lineage)
    }.toMap

    debug {
      logger.info(s"Cells done! ${cells.size} cells created.")
    }

    // FIXME: Currently, we are assuming that the time instants are continuous from 0 to endtime.
    // Getting intervals...
    val endtime                                = if (S.endtime > 0) S.endtime
    else {
      trajs.map {
        point =>
          PF_Utils.getTime(point)
      }.max()
    }
    debug {
      logger.info(s"Using endtime = $endtime")
    }
    val times_prime                            = (0 to endtime).toList
    implicit val intervals: Map[Int, Interval] = Interval.intervalsBySize(times_prime, S.step)

    debug {
      logger.info(s"Intervals done!")
      save(s"/tmp/Intervals_${S.appId}.tsv") {
        intervals.values.toList
          .sortBy(_.index)
          .map(interval => interval.toText)
      }
    }

    // Assigning points to spatio-temporal partitions...
    val trajs_prime = trajs
      .filter(point => PF_Utils.getTime(point) <= endtime)
      .mapPartitions {
        rows =>
          rows.flatMap {
            point =>
              val tid     = PF_Utils.getTime(point)
              val t_index = Interval.findTimeInstant(tid)

              val env = point.getEnvelopeInternal
              env.expandBy(S.epsilon)
              quadtree.findZones(new QuadRectangle(env)).asScala.map {
                cell =>
                  val cid     = cell.partitionId.toInt
                  val s_index = cells(cid)

                  val st_index = Encoder.encode(s_index.id, t_index.index)
                  (st_index, point)
              }

          }
      }
      .cache

    debug {
      logger.info(s"Partitions done!")
    }

    // Creating cubes...
    val cubes: Map[Int, Cube]         = trajs_prime
      .map { _._1 }
      .distinct()
      .collect()
      .zipWithIndex
      .map {
        case (st_index, cube_id) =>
          val (s_index, t_index) = Encoder.decode(st_index)
          val cell               = cells(s_index)
          val interval           = intervals(t_index)

          cube_id -> Cube(cube_id, cell, interval, st_index)
      }
      .toMap
    // Creating reverse mapping from st_index to cube_id...
    val cubes_reversed: Map[Int, Int] = cubes.map {
      case (cube_id, cube) =>
        cube.st_index -> cube_id
    }.toMap

    debug {
      logger.info(s"Cubes done!")
    }

    // Re-partitioning trajectory points by cube_id...
    val trajs_partitioned = trajs_prime
      .map {
        case (st_index, point) =>
          val cube_id = cubes_reversed(st_index)
          (cube_id, point)
      }
      .partitionBy(SimplePartitioner(cubes.size))
      .map(_._2)
      .filter {
        p =>
          val data = p.getUserData.asInstanceOf[Data]
          data.tid <= endtime
      }.cache()
    trajs_partitioned.count()

    debug {
      logger.info(s"Re-partitions done!")
    }

    (trajs_partitioned, cubes, quadtree)
  }

  def getDynamicIntervalCubes(trajs: RDD[Point])(implicit
    S: Settings
  ): (RDD[Point], Map[Int, Cube], StandardQuadTree[Point]) = {
    // Building quadtree...
    debug {
      logger.info(s"Running Dynamic Interval Cube Partitioner...")
    }
    implicit var G: GeometryFactory        = S.geofactory
    val (quadtree, cells, rtree, universe) =
      Quadtree.build(trajs, new Envelope(), capacity = S.scapacity, fraction = S.fraction)

    debug {
      logger.info(s"Quadtree done!")
      save(s"/tmp/Cells_${S.appId}.wkt") {
        cells.values.map {
          cell =>
            s"${cell.toText}"
        }.toList
      }
    }

    val trajsSRDD = trajs
      .mapPartitions {
        iter =>
          iter.map {
            point =>
              val cid = rtree
                .query(point.getEnvelopeInternal())
                .asScala
                .head
                .asInstanceOf[Int]
              (cid, point)
          }
      }
      .partitionBy(SimplePartitioner(cells.size))
      .map(_._2)
      .cache()
    val nTrajs    = trajsSRDD.count()

    debug {
      logger.info(s"Spatial partitioning done!")
    }

    val histogram: Array[Bin] = trajsSRDD
      .mapPartitions {
        it =>
          val partitionId = TaskContext.getPartitionId()
          it.map {
            point =>
              point.getUserData().asInstanceOf[Data].tid
          }.toList
            .groupBy(tid => tid)
            .map {
              case (tid, list) =>
                (tid, list.size)
            }
            .toIterator
      }
      .groupByKey()
      .map {
        case (tid, counts) =>
          val total = counts.sum
          Bin(tid, total)
      }
      .collect()

    debug {
      save(s"/tmp/histogram_${S.appId}.tsv") {
        histogram
          .sortBy(_.instant)
          .map(bin => s"${bin.toString()}\n")
          .toList
      }
      logger.info("Histogram done!")
    }

    implicit val intervals = Interval
      .groupInstants(histogram.sortBy(_.instant).toSeq, capacity = S.tcapacity)
      .map {
        group =>
          val begin           = group.head.instant
          val end             = group.last.instant
          val number_of_times = group.map(_.count).sum

          (begin, end, number_of_times)
      }
      .zipWithIndex
      .map {
        case ((begin, end, number_of_times), index) =>
          (index, Interval(index, begin, end, number_of_times))
      }
      .toMap

    debug {
      save(s"/tmp/Intervals_${S.appId}.tsv") {
        intervals.values.toList
          .sortBy(_.index)
          .map(interval => interval.toText)
      }
      logger.info("Intervals done!")
    }

    val trajsSTRDD_prime = trajsSRDD
      .mapPartitionsWithIndex {
        (spatial_index, it) =>
          it.map {
            point =>
              val data = point.getUserData().asInstanceOf[Data]
              val tid  = data.tid

              val temporal_index = Interval.findTimeInstant(tid).index
              val ST_Index       = Encoder.encode(spatial_index, temporal_index)
              (ST_Index, point)
          }
      }
      .cache()

    debug {
      logger.info(s"Spatio-temporal index done!")
    }

    val cubes: Map[Int, Cube]         = trajsSTRDD_prime
      .map { case (st_index, _) => st_index }
      .distinct()
      .collect()
      .zipWithIndex
      .map {
        case (st_index, index) =>
          val (s_index, t_index) = Encoder.decode(st_index)
          val cell               = cells(s_index)
          val interval           = intervals(t_index)

          index -> Cube(index, cell, interval, st_index)
      }
      .toMap
    val cubes_reversed: Map[Int, Int] = cubes.map {
      case (cube_id, cube) =>
        cube.st_index -> cube_id
    }.toMap

    debug {
      logger.info(s"Cubes done!")
    }

    val trajsSTRDD = trajsSTRDD_prime
      .map { case (st_index, point) => (cubes_reversed(st_index), point) }
      .partitionBy(SimplePartitioner(cubes.size))
      .map(_._2)
      .cache()
    trajsSTRDD.count()

    debug {
      logger.info(s"Re-partition done!")
    }

    (trajsSTRDD, cubes, quadtree)
  }
}
