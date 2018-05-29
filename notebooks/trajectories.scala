import org.apache.spark.sql.simba.{Dataset, SimbaSession}
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.types.{StructType, StructField, LongType, DoubleType, StringType}

import org.apache.spark.sql.functions._
import scala.collection.JavaConverters._

// spark://169.235.27.134:7077
val simba = SimbaSession.builder().master("spark://169.235.27.134:7077").appName("Trajectories").config("simba.index.partitions", 1024).config("spark.cores.max", 32).getOrCreate()

case class ST_Point(tid: Long, oid: Long, x: Double, y: Double, t: String)

import simba.implicits._
import simba.simbaImplicits._

val tid = StructField("tid", LongType)
val oid = StructField("oid", LongType)
val x = StructField("x", DoubleType)
val y = StructField("y", DoubleType)
val t = StructField("t", StringType)
val point_schema = StructType(Array(tid, oid, x, y, t))

val filename = "/home/acald013/tmp/trajectories.csv"

val pointset = simba.read.option("header", "false").option("sep", ",").schema(point_schema).csv(filename).as[ST_Point].cache()
val nPointset = pointset.count()

pointset.show(truncate = false)

pointset.orderBy($"tid", $"oid").show(10, truncate = false)

val g = pointset.groupBy($"tid", $"oid").count().cache()
g.count()
g.select(sum($"count")).show
g.show

pointset.select($"tid", $"oid").count
pointset.select($"tid", $"oid").distinct.count
pointset.select($"oid").distinct.count
pointset.select($"tid").distinct.count

simba.close()
