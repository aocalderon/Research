import org.apache.spark.sql.types.StructType
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, CircleRDD, PointRDD}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.enums.{GridType, IndexType}
import com.vividsolutions.jts.geom.{Geometry, Envelope, Coordinate, Point, Polygon}
import com.vividsolutions.jts.geom.GeometryFactory
import org.datasyslab.geospark.spatialPartitioning.quadtree._
import org.datasyslab.geospark.spatialPartitioning.QuadtreePartitioning
import scala.io.Source
import edu.ucr.dblab.{StandardQuadTree, QuadRectangle}

object Quadtree {

  def readGrids(filename: String, offset: Int, delimiter: String = "\t"): Vector[String] = {
    val envelopes = Source.fromFile("/tmp/envelopes.tsv")
    val grids = envelopes.getLines.map{ line =>
      val arr = line.split(delimiter)
      arr(offset)
    }.toVector
    envelopes.close
    grids
  }
  
  def create[T](boundary: Envelope, maxItems: Int, grids: Vector[String]): StandardQuadTree[T] = {
    val maxLevel = grids.map(_.size).max
    val quadtree = new StandardQuadTree[T](new QuadRectangle(boundary), 0, maxItems, maxLevel)
    quadtree.split()
    for(grid <- grids.sorted){
      val lineage = grid.map(_.toInt - 48)
      var current = quadtree
      for(position <- lineage.slice(0, lineage.size - 1)){
        val regions = current.getRegions()
        current = regions(position)
        if(current.getRegions == null){
          current.split()
        }
      }
    }
    quadtree.assignPartitionLineage()
    quadtree.assignPartitionIds()

    quadtree
  }
}
