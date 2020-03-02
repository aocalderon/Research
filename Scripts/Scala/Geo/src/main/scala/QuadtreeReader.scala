//**
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
//**
object QuadtreeReader {
  def main(args: Array[String]): Unit = {
    //**
    implicit val spark = SparkSession.builder().appName("QuadtreeReader").config("spark.serializer", classOf[KryoSerializer].getName).config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).getOrCreate()
    import spark.implicits._
    //**
    case class Grid(id: Int, lineage: String, envelope: Envelope)
    val envelopes = Source.fromFile("/tmp/envelopes.tsv")
    val grids = envelopes.getLines.map{ line =>
      val arr = line.split("\t")
      val id = arr(0).toInt
      val lineage = arr(1)
      val envelope = new Envelope(arr(2).toDouble, arr(4).toDouble, arr(3).toDouble, arr(5).toDouble)
      Grid(id, lineage, envelope)
    }.toList
    //**
    val samples = grids.map(_.envelope)
    val minX = grids.map(_.envelope.getMinX()).min
    val minY = grids.map(_.envelope.getMinY()).min
    val maxX = grids.map(_.envelope.getMaxX()).max
    val maxY = grids.map(_.envelope.getMaxY()).max
    //**
    val boundary = new Envelope(minX, maxX, minY, maxY)
    val partitions = grids.size
    val maxLevel = grids.map(_.lineage.size).max
    val quadtree = new StandardQuadTree[Int](new QuadRectangle(boundary), 0, 1, maxLevel)
    //**
    val geofactory = new GeometryFactory()
    val wkt = grids.map{ g =>
      val x1 = g.envelope.getMinX()
      val x2 = g.envelope.getMaxX()
      val y1 = g.envelope.getMinY()
      val y2 = g.envelope.getMaxY()
      val coords = Array(new Coordinate(x1,y1),new Coordinate(x2,y1),new Coordinate(x2,y2),new Coordinate(x1,y2),new Coordinate(x1,y1))
      val polygon = geofactory.createPolygon(coords)
      polygon.setUserData(s"${g.id}\t${g.lineage}")
      s"${polygon.toText()}\t${polygon.getUserData.toString()}\n"
    }
    val f = new java.io.PrintWriter("edgesEnvelopes.wkt")
    f.write(wkt.mkString(""))
    f.close()
    for(sample <- samples){
      println(sample)
      quadtree.insert(new QuadRectangle(sample), 1)
      println(quadtree.getLeafZones.size())
    }
    //**
    quadtree.assignPartitionLineage()
    quadtree.assignPartitionIds()
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
    //**
    spark.close
    envelopes.close
    //**
  }
}
