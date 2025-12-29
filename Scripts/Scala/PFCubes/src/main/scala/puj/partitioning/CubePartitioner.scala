package puj.partitioning

import org.locationtech.jts.geom.{GeometryFactory, Point}

import org.apache.spark.rdd.RDD
import org.apache.logging.log4j.scala.Logging

import edu.ucr.dblab.pflock.sedona.quadtree.{StandardQuadTree, QuadRectangle}

import puj.Settings
import puj.PF_Utils
import puj.Utils.{SimplePartitioner, Data, debug}

import scala.collection.JavaConverters.asScalaBufferConverter
import org.locationtech.jts.geom.Envelope
import puj.Utils.saveAsTSV
import org.apache.spark.TaskContext

case class Cube(id: Int, cell: Cell, interval: Interval, st_index: Int) {
  def wkt: String = {
    val wkt = cell.wkt
    val beg = interval.begin
    val dur = interval.duration

    s"$wkt\t$id\t$beg\t$dur"
  }
}

object CubePartitioner extends Logging {

  /** Partitions trajectory points into spatio-temporal cubes based on a quadtree for spatial partitioning and fixed time intervals for temporal partitioning.
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
  def getFixedIntervalCubes(trajs: RDD[Point])(implicit S: Settings, G: GeometryFactory): (RDD[Point], Map[Int, Cube], StandardQuadTree[Point]) = {

    // Building quadtree...
    val sample   = trajs.sample(withReplacement = false, fraction = S.fraction, seed = 42).collect()
    val universe = PF_Utils.getEnvelope(trajs)
    universe.expandBy(S.epsilon * 2.0)
    val quadtree: StandardQuadTree[Point] = new StandardQuadTree[Point](new QuadRectangle(universe), 0, S.scapacity, 16)
    sample.foreach { point =>
      quadtree.insert(new QuadRectangle(point.getEnvelopeInternal), point)
    }
    quadtree.assignPartitionIds()
    quadtree.assignPartitionLineage()
    quadtree.dropElements()

    debug {
      logger.info(s"Quadtree done!")
    }

    // Getting cells...
    implicit val cells: Map[Int, Cell] = quadtree.getLeafZones.asScala.map { leaf =>
      val envelope = leaf.getEnvelope
      val id       = leaf.partitionId.toInt

      id -> Cell(id, envelope, leaf.lineage)
    }.toMap

    debug {
      logger.info(s"Cells done!")
    }

    // FIXME: Currently, we are assuming that the time instants are continuous from 0 to endtime.
    // Getting intervals...
    val times_prime                            = (0 to S.endtime).toList
    implicit val intervals: Map[Int, Interval] = Interval.intervalsBySize(times_prime, S.step)

    debug {
      logger.info(s"Intervals done!")
    }

    // Assigning points to spatio-temporal partitions...
    val trajs_prime = trajs
      .filter(point => PF_Utils.getTime(point) <= S.endtime)
      .mapPartitions { rows =>
        rows.flatMap { point =>
          val tid     = PF_Utils.getTime(point)
          val t_index = Interval.findTimeInstant(tid)

          val env = point.getEnvelopeInternal
          env.expandBy(S.epsilon)
          quadtree.findZones(new QuadRectangle(env)).asScala.map { cell =>
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
    val cubes: Map[Int, Cube] = trajs_prime
      .map { _._1 }
      .distinct()
      .collect()
      .zipWithIndex
      .map { case (st_index, cube_id) =>
        val (s_index, t_index) = Encoder.decode(st_index)
        val cell               = cells(s_index)
        val interval           = intervals(t_index)

        cube_id -> Cube(cube_id, cell, interval, st_index)
      }
      .toMap
    // Creating reverse mapping from st_index to cube_id...
    val cubes_reversed: Map[Int, Int] = cubes.map { case (cube_id, cube) =>
      cube.st_index -> cube_id
    }.toMap

    debug {
      logger.info(s"Cubes done!")
    }

    // Re-partitioning trajectory points by cube_id...
    val trajs_partitioned = trajs_prime
      .map { case (st_index, point) =>
        val cube_id = cubes_reversed(st_index)
        (cube_id, point)
      }
      .partitionBy(SimplePartitioner(cubes.size))
      .map(_._2)
      .filter { p =>
        val data = p.getUserData.asInstanceOf[Data]
        data.tid <= S.endtime
      }

    debug {
      logger.info(s"Re-partitions done!")
    }

    (trajs_partitioned, cubes, quadtree)
  }

  def getDynamicIntervalCubes(trajs: RDD[Point])(implicit S: Settings, G: GeometryFactory): (RDD[Point], Map[Int, Cube], StandardQuadTree[Point]) = {
    // Building quadtree...
    val (quadtree, cells, rtree, universe) =
      Quadtree.build(trajs, new Envelope(), capacity = S.scapacity, fraction = S.fraction)
    logger.info(s"Quadtree built with ${cells.size} cells")

    val trajsSRDD = trajs
      .mapPartitions { iter =>
        iter.map { point =>
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
    val nTrajs = trajsSRDD.count()
    logger.info(s"Trajectories repartitioned into SRDD with $nTrajs points")

    val histogram: Array[Bin] = trajsSRDD.mapPartitions { it =>
        val partitionId = TaskContext.getPartitionId()
        it.map { point =>
          point.getUserData().asInstanceOf[Data].tid
        }.toList
          .groupBy(tid => tid)
          .map { case (tid, list) =>
            (tid, list.size)
          }
          .toIterator
      }
      .groupByKey()
      .map { case (tid, counts) =>
        val total = counts.sum
        Bin(tid, total)
      }.collect()
    logger.info("Temporal histogram computed")

    debug {
      saveAsTSV(
        "/tmp/histogram.tsv",
        histogram
          .sortBy(_.instant)
          .map(bin => s"${bin.toString()}\n")
          .toList
      )
      logger.info("Temporal histogram TSV file saved for debugging")
    }

    implicit val intervals = Interval
      .groupInstants(histogram.sortBy(_.instant).toSeq, capacity = S.tcapacity)
      .map { group =>
        val begin           = group.head.instant
        val end             = group.last.instant
        val number_of_times = group.map(_.count).sum

        (begin, end, number_of_times)
      }
      .zipWithIndex
      .map { case ((begin, end, number_of_times), index) =>
        (index, Interval(index, begin, end, number_of_times))
      }
      .toMap

    debug {
      saveAsTSV(
        "/tmp/Intervals.tsv",
        intervals.values.toList
          .sortBy(_.index)
          .map(interval => interval.toText)
      )
      logger.info("Temporal intervals TSV file saved for debugging")
    }
    logger.info(s"Total temporal intervals created: ${intervals.size}")

    val temporal_bounds = intervals.values.map(_.begin).toArray.sorted

    val pointsSTRDD_prime = trajsSRDD
      .mapPartitionsWithIndex { (spatial_index, it) =>
        it.map { point =>
          val data = point.getUserData().asInstanceOf[Data]
          val tid  = data.tid

          val temporal_index = Interval.findTimeInstant(tid).index
          val ST_Index       = Encoder.encode(spatial_index, temporal_index)
          (ST_Index, point)
        }
      }
      .cache()

    val cubes: Map[Int, Cube] = pointsSTRDD_prime
      .map { case (st_index, _) => st_index }
      .distinct()
      .collect()
      .zipWithIndex.map{ case (st_index, index) => 
        val (s_index, t_index) = Encoder.decode(st_index)
        val cell               = cells(s_index)
        val interval           = intervals(t_index)

        index -> Cube(index, cell, interval, st_index)
      }
      .toMap
    val cubes_reversed: Map[Int, Int] = cubes.map { case (cube_id, cube) =>
      cube.st_index -> cube_id
    }.toMap
    logger.info(s"Total cubes created: ${cubes.size}")

    val pointsSTRDD = pointsSTRDD_prime
      .map { case (st_index, point) => (cubes_reversed(st_index), point) }
      .partitionBy(SimplePartitioner(cubes.size))
      .map(_._2)
      .cache()
    val nPointsSTRDD = pointsSTRDD.count()
    logger.info(s"Points repartitioned into STRDD with $nPointsSTRDD points")

    debug {
      pointsSTRDD
        .mapPartitions { points =>
          val partitionId    = TaskContext.getPartitionId()
          val cube           = cubes(partitionId)
          val st_index       = cube.st_index
          val (s_index, t_index) = Encoder.decode(st_index)
          val wkts           = points
            .map { point =>
              val i   = point.getUserData().asInstanceOf[Data].oid
              val x   = point.getX
              val y   = point.getY
              val t   = point.getUserData().asInstanceOf[Data].tid
              val wkt = point.toText()
              s"$i\t$x\t$y\t$t\t$s_index\t$t_index\t$st_index\t$wkt\n"
            }
            .mkString("")
          Iterator((s_index, wkts))
        }
        .collect()
        .groupBy(_._1)
        .foreach { case (s_index, wkts) =>
          saveAsTSV(
            s"/tmp/STRDD_$s_index.wkt",
            wkts.map(_._2).toList
          )
        }
      logger.info("STRDD WKT file saved for debugging")

      pointsSTRDD
        .mapPartitions { points =>
          val partitionId        = TaskContext.getPartitionId()
          val cube               = cubes(partitionId)
          val st_index           = cube.st_index
          val (s_index, t_index) = Encoder.decode(cube.st_index)
          val cell               = cube.cell
          val interval           = cube.interval

          val wkt = s"${cell.wkt}\t$st_index\t$s_index\t$t_index\t${interval.begin}\t${interval.duration}\n"
          Iterator((s_index, wkt))
        }
        .collect()
        .groupBy(_._1)
        .foreach { case (s_index, wkts) =>
          saveAsTSV(
            s"/tmp/Boxes_$s_index.wkt",
            wkts.map(_._2).toList
          )
        }
      logger.info("Boxes WKT file saved for debugging")
    }

    ???
  }
}
