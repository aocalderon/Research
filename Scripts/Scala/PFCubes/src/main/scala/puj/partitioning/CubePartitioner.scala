package puj.partitioning

import org.locationtech.jts.geom.{GeometryFactory, Point}

import org.apache.spark.rdd.RDD
import org.apache.logging.log4j.scala.Logging

import edu.ucr.dblab.pflock.sedona.quadtree.{StandardQuadTree, QuadRectangle}

import puj.Settings
import puj.PF_Utils
import puj.Utils.{SimplePartitioner, Data, debug}

import scala.collection.JavaConverters.asScalaBufferConverter

case class Cube(id: Int, cell: Cell, interval: Interval, st_index: Int) {
  def wkt: String = {
    val wkt = cell.wkt
    val beg = interval.begin
    val dur = interval.duration

    s"$wkt\t$id\t$beg\t$dur"
  }
}

object CubePartitioner extends Logging {

    /** Partitions trajectory points into spatio-temporal cubes based on a quadtree 
      *     for spatial partitioning and fixed time intervals for temporal partitioning.
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
    def getFixedIntervalCubes( trajs: RDD[(Int, Point)] )
        (implicit S: Settings, G: GeometryFactory): (RDD[Point], Map[Int, Cube], StandardQuadTree[Point]) = {
        
        // Building quadtree...
        val sample   = trajs.sample(withReplacement = false, fraction = S.fraction, seed = 42).collect()
        val universe = PF_Utils.getEnvelope(trajs)
        universe.expandBy(S.epsilon * 2.0)
        val quadtree: StandardQuadTree[Point] = new StandardQuadTree[Point](new QuadRectangle(universe), 0, S.scapacity, 16)
        sample.foreach { case (_, point) =>
            quadtree.insert(new QuadRectangle(point.getEnvelopeInternal), point)
        }
        quadtree.assignPartitionIds()
        quadtree.assignPartitionLineage()
        quadtree.dropElements()

      debug{
        logger.info(s"Quadtree done!")
      }

        // Getting cells...
        implicit val cells: Map[Int, Cell] = quadtree.getLeafZones.asScala.map { leaf =>
            val envelope = leaf.getEnvelope
            val id       = leaf.partitionId.toInt

            id -> Cell(id, envelope, leaf.lineage)
        }.toMap

      debug{
        logger.info(s"Cells done!")
      }


        // FIXME: Currently, we are assuming that the time instants are continuous from 0 to endtime.
        // Getting intervals...
        val times_prime = (0 to S.endtime).toList
        implicit val intervals: Map[Int, Interval] = Interval.intervalsBySize(times_prime, S.step)

      debug{
        logger.info(s"Intervals done!")
      }

        // Assigning points to spatio-temporal partitions...
        val trajs_prime = trajs
            .filter(_._1 <= S.endtime)
            .mapPartitions { rows =>
                rows.flatMap { case (_, point) =>
                    val tid = PF_Utils.getTime(point)
                    val t_index = Interval.findTimeInstant(tid)

                    val env   = point.getEnvelopeInternal
                    env.expandBy(S.epsilon)
                    quadtree.findZones(new QuadRectangle(env)).asScala.map { cell =>
                        val cid = cell.partitionId.toInt
                        val s_index = cells(cid)

                        val st_index = Encoder.encode(s_index.id, t_index.index)
                        (st_index, point)
                    }
                
                }
            }
            .cache

      debug{
        logger.info(s"Partitions done!")
      }

        // Creating cubes...
        val cubes: Map[Int, Cube] = trajs_prime.map { _._1 }.distinct().collect()
            .zipWithIndex.map{ case (st_index, cube_id) =>
                val (s_index, t_index) = Encoder.decode(st_index)
                val cell = cells(s_index)
                val interval = intervals(t_index)

                cube_id -> Cube(cube_id, cell, interval, st_index)
            }.toMap
        // Creating reverse mapping from st_index to cube_id...
        val cubes_reversed: Map[Int, Int] = cubes.map { case (cube_id, cube) =>
            cube.st_index -> cube_id
        }.toMap

      debug{
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

      debug{
        logger.info(s"Re-partitions done!")
      }

        (trajs_partitioned, cubes, quadtree)
    }
}
