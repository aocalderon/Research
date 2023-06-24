package edu.ucr.dblab.tester3d

import com.astrolabsoftware.spark3d.spatialPartitioning._
import com.astrolabsoftware.spark3d.geometryObjects._

import scala.io.Source
import scala.util.Random

object Partitioner3D {
  def main(args: Array[String]): Unit = {
    val limit = 500.0
    val points = (0 to 1000).map{ i =>
      val x = Random.nextDouble() * limit
      val y = Random.nextDouble() * limit
      val z = Random.nextDouble() * limit

      new Point3D(x, y, z, false)
    }.toList

    val tree_space = BoxEnvelope.apply(0.0, limit, 0.0, limit, 0.0, limit)
    val tree = new Octree(new BoxEnvelope(tree_space), 0, null, 250, 16)

    val partitioning = OctreePartitioning(points.map(_.getEnvelope), tree)
    val partitioner = new OctreePartitioner(partitioning.getPartitionTree, partitioning.getGrids)

    val out = points.flatMap { point =>
      partitioner.placeObject(point).map{ case(id, envelope) =>
        (id, point)
        s"${point.x}\t${point.y}\t${point.z}\t${id}\n"
      }
    }.mkString("")

    val f = new java.io.PrintWriter("/tmp/test3d.tsv")
    f.write(out)
    f.close
  }
}
