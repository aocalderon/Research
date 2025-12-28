package puj.partitioning

import org.locationtech.jts.geom.{GeometryFactory, Point}

import org.apache.spark.rdd.RDD
import org.apache.logging.log4j.scala.Logging

import edu.ucr.dblab.pflock.sedona.quadtree.Quadtree.Cell   
import edu.ucr.dblab.pflock.sedona.quadtree.{StandardQuadTree, QuadRectangle}

import puj.Settings
import puj.PF_Utils
import puj.Utils.{SimplePartitioner, Data}

import scala.collection.JavaConverters.asScalaBufferConverter

case class Cube(id: Int, cell: Cell, interval: Interval)

object CubePartitioner extends Logging {

    def getFixedIntervalCubes(trajs: RDD[(Int, Point)])(implicit S: Settings, G: GeometryFactory): (RDD[Point], Map[Int, (Int, Int)]) = {
        // Getting cells...
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

        val cells: Map[Int, Cell] = quadtree.getLeafZones.asScala.map { leaf =>
            val envelope = leaf.getEnvelope
            val id       = leaf.partitionId.toInt

            id -> Cell(id, envelope, leaf.lineage)
        }.toMap

        // Getting intervals...
        val times_prime = (0 to S.endtime).toList
        val time_partitions = PF_Utils.cut(times_prime, S.step)

        // Assigning points to spatial partitions...
        val trajs_prime = trajs
            .filter(_._1 <= S.endtime)
            .mapPartitions { rows =>
                rows.flatMap { case (_, point) =>
                    val tpart = time_partitions(PF_Utils.getTime(point))
                    val env   = point.getEnvelopeInternal
                    env.expandBy(S.epsilon)
                    quadtree.findZones(new QuadRectangle(env)).asScala.map { x =>
                        ((x.partitionId.toInt, tpart), point)
                    }
                }
            }
            .cache

        // Assigning unique IDs to spatiotemporal partitions...
        val cubes_ids: Map[(Int, Int), Int] = trajs_prime.map { _._1 }.distinct().collect().sortBy(_._1).sortBy(_._2).zipWithIndex.toMap
        // Inverse map for spatiotemporal partitions...
        val cubes_ids_inverse: Map[Int, (Int, Int)] = cubes_ids.map { case (key, value) => value -> key }

        // Repartitioning trajectories according to spatiotemporal partitions...
        val trajs_partitioned = trajs_prime
            .map { case (tuple_id, point) =>
                val cube_id = cubes_ids(tuple_id)
                (cube_id, point)
            }
            .partitionBy(SimplePartitioner(cubes_ids.size))
            .map(_._2)
            .filter { p =>
                val data = p.getUserData.asInstanceOf[Data]
                data.tid <= S.endtime
            }

        (trajs_partitioned, cubes_ids_inverse)
    }
}
