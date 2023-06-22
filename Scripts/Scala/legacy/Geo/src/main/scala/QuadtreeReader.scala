import scala.collection.JavaConverters._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, CircleRDD, PointRDD}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.enums.{GridType, IndexType}
import com.vividsolutions.jts.geom.{Envelope, Coordinate, Point, Polygon}
import com.vividsolutions.jts.geom.GeometryFactory
import org.datasyslab.geospark.spatialPartitioning.quadtree._
import org.datasyslab.geospark.spatialPartitioning.QuadtreePartitioning
import scala.io.Source
import edu.ucr.dblab.{StandardQuadTree, QuadRectangle}

object QuadtreeReader {
  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession.builder()
      .appName("QuadtreeReader")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .getOrCreate()
    import spark.implicits._

    val envelopes = Source.fromFile("/tmp/envelopes.tsv")
    .getLines.map{ line =>
      val arr = line.split("\t")
      new Envelope(arr(2).toDouble, arr(4).toDouble, arr(3).toDouble, arr(5).toDouble)
    }.toList
    
    val minX = envelopes.map(_.getMinX()).min
    val minY = envelopes.map(_.getMinY()).min
    val maxX = envelopes.map(_.getMaxX()).max
    val maxY = envelopes.map(_.getMaxY()).max

    val geofactory = new GeometryFactory()
    val wkt = envelopes.map{ g =>
      val x1 = g.getMinX()
      val x2 = g.getMaxX()
      val y1 = g.getMinY()
      val y2 = g.getMaxY()
      val coords = Array(new Coordinate(x1,y1),
        new Coordinate(x2,y1),
        new Coordinate(x2,y2),
        new Coordinate(x1,y2),
        new Coordinate(x1,y1))
      val polygon = geofactory.createPolygon(coords)
      s"${polygon.toText()}\n"
    }
    val f = new java.io.PrintWriter("edgesEnvelopes.wkt")
    f.write(wkt.mkString(""))
    f.close()

    val boundary = new Envelope(minX, maxX, minY, maxY)
    val cells = Quadtree.readGrids("/tmp/envelopes.tsv", 1) 
    val quadtree = Quadtree.create(boundary, 1, cells)

    val grids2 = quadtree.getLeafZones.asScala
    val wkt2 = grids2.map{ g =>
      val x1 = g.getEnvelope.getMinX()
      val x2 = g.getEnvelope.getMaxX()
      val y1 = g.getEnvelope.getMinY()
      val y2 = g.getEnvelope.getMaxY()
      val coords = Array(new Coordinate(x1,y1),new Coordinate(x2,y1),new Coordinate(x2,y2),new Coordinate(x1,y2),new Coordinate(x1,y1))
      val polygon = geofactory.createPolygon(coords)
      polygon.setUserData(s"${g.partitionId}\t${g.lineage}")
      s"${polygon.toText()}\t${polygon.getUserData.toString()}\n"
    }
    val f2 = new java.io.PrintWriter("edgesEnvelopes2.wkt")
    f2.write(wkt2.mkString(""))
    f2.close()

    spark.close
  }
}
