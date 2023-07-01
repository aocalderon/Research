package edu.ucr.dblab.tester3d

import com.astrolabsoftware.spark3d.spatialPartitioning._
import com.astrolabsoftware.spark3d.geometryObjects._

import tech.tablesaw.api._
import tech.tablesaw.plotly.Plot
import tech.tablesaw.plotly.api._

import scala.util.Random
import java.io.PrintWriter

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

    val partitioned_points = points.flatMap { point =>
      partitioner.placeObject(point).map{ case(vid, envelope) =>
        (point, vid)
      }
    }
    val out = partitioned_points.map{ case(point, vid) =>
      s"${point.x},${point.y},${point.z},${vid}\n"
    }.mkString("")

    val f = new PrintWriter("/tmp/test3d.csv")
    f.write("x,y,z,vid\n")
    f.write(out)
    f.close

    val x  = DoubleColumn.create("x")
    val y  = DoubleColumn.create("y")
    val z  = DoubleColumn.create("z")
    val id = IntColumn.create("id")
    partitioned_points.foreach{ case(point, vid) =>
      x.append(point.x)
      y.append(point.y)
      z.append(point.z)
      id.append(vid)
    }
    val data = Table.create("points", x, y, z, id)

    val scatter3d = Scatter3DPlot.create("points", data, "x", "y", "z", "id")
    Plot.show(scatter3d)

    val scatter2d = ScatterPlot.create("points", data, "x", "y")
    Plot.show(scatter2d, "scatter2d.png")
  }
}
