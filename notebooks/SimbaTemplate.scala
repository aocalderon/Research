import org.apache.spark.sql.simba.{Dataset, SimbaSession}
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.types.{StructType, StructField, LongType, DoubleType, IntegerType}

import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._

// spark://169.235.27.134:7077
val simba = SimbaSession.builder().master("local[*]").appName("MaximalDistanceFinder").config("simba.index.partitions", 1024).config("spark.cores.max", 32).getOrCreate()

case class ST_Point(id: Long, x: Double, y: Double, t: Int = -1)
case class Flock(start: Int, end: Int, ids: String, x: Double = 0.0, y: Double = 0.0)

import simba.implicits._
import simba.simbaImplicits._

val id = StructField("id", LongType)
val x = StructField("x", DoubleType)
val y = StructField("y", DoubleType)
val t = StructField("t", IntegerType)
val point_schema = StructType(Array(id, x, y, t))

val filename = "/home/and/Documents/PhD/Research/Datasets/Berlin/berlin0-2.tsv"

val pointset = simba.read.option("header", "false").option("sep", "\t").schema(point_schema).csv(filename).as[ST_Point].cache()
val nPointset = pointset.count()

val epsilon = 50
val sample = pointset.filter(_.t == 0).sample(false, 0.5, 42).cache()
sample.orderBy("id").show(truncate =false)
val p1 = sample.select("id", "x", "y").toDF("id1", "x1", "y1")
p1.index(RTreeType, "rtP1", Array("x1", "y1"))
val p2 = sample.select("id", "x", "y").toDF("id2", "x2", "y2")
p2.index(RTreeType, "rtP2", Array("x2", "y2"))
val join = p1.distanceJoin(p2, Array("x1", "y1"), Array("x2", "y2"), epsilon).filter(j => j.getLong(0) < j.getLong(3)).cache()
val nJoin = join.count()
join.orderBy("id1", "id2").show(truncate = false)

simba.close()
