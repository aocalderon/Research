import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

/**
  * Created by and on 3/20/17.
  */

object Partitioner {
  case class SP_Point(tid: Int, oid: Int, x: Double, y: Double, t: String)
  
  def toWKT(minx: Double, miny: Double, maxx: Double, maxy: Double): String = "POLYGON (( %f %f, %f %f, %f %f, %f %f, %f %f ))".
    format(
      minx, maxy,
      maxx, maxy,
      maxx, miny,
      minx, miny,
      minx, maxy
    )  

  def main(args: Array[String]): Unit = {
    val simba = SimbaSession
      .builder()
      .master("local[*]")
      .appName("Partitioner")
      .config("simba.index.partitions", "10")
      .getOrCreate()

    import simba.implicits._
    import simba.simbaImplicits._
    
    val schema = ScalaReflection.schemaFor[SP_Point].dataType.asInstanceOf[StructType]
            
    val filename = "/tmp/t.csv"
    val points = simba.read.option("header", "false").schema(schema).csv(filename). as[SP_Point].cache
    println(points.count())
    //val sample = points.sample(false, 0.5)//.range(Array("x", "y"), Array(-339220.0, 4444725.0), Array(-309375.0, 4478070.0)).as[SP_Point].cache
    //println(sample.count())
    println(points.rdd.getNumPartitions)
    val data = points.index(RTreeType, "rtSample", Array("x", "y")).cache
    println(data.rdd.getNumPartitions)

    val mbrs = data.rdd.mapPartitionsWithIndex{ (index, iterator) =>
      var min_x: Double = Double.MaxValue
      var min_y: Double = Double.MaxValue
      var max_x: Double = Double.MinValue
      var max_y: Double = Double.MinValue

      var size: Int = 0

      iterator.toList.foreach{row =>
        val x = row.x
        val y = row.y
        if(x < min_x){
          min_x = x
        }
        if(y < min_y){
          min_y = y
        }
        if(x > max_x){
          max_x = x
        }
        if(y > max_y){
          max_y = y
        }
        size += 1
      }
      List("%s:%d:%d".format(toWKT(min_x,min_y,max_x,max_y), index, size)).iterator
    }
    mbrs.foreach(println)	
    
    import java.io.{BufferedWriter, FileOutputStream, OutputStreamWriter}
    var writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/tmp/mbrs.wkt")))
    mbrs.map(mbr => s"$mbr\n").collect.foreach(writer.write)
    writer.close()	

    simba.stop()
  }
}
