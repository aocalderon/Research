import org.apache.spark.sql.simba.SimbaSession

val cores = 32
val partitions = 1024
val master = "spark://169.235.27.134:7077"

val simba = SimbaSession.builder().master(master).appName("FlockFinder").config("simba.index.partitions", partitions).config("spark.cores.max", cores).getOrCreate()

import simba.implicits._
import simba.simbaImplicits._

case class ST_Point(id: Int, x: Double, y: Double, t: Int)
case class Flock(start: Int, end: Int, ids: String, lon: Double = 0.0, lat: Double = 0.0)

val currentPoints = (0 until 10).map(p => ST_Point(p.toInt, p * 100.0, p * 100.0, p.toInt)).toDS
val C = (0 until 10).map(c => Flock(c.toInt, c.toInt, Seq.fill(3 + scala.util.Random.nextInt(7))(scala.util.Random.nextInt(10)).toSet.toList.sorted.mkString(" "))).toDS

val C_prime = C.map(c => (c.ids, c.ids.split(" ").map(_.toInt))).toDF("ids", "idsList").withColumn("id", explode($"idsList")).select("ids", "id")
val P_prime = currentPoints.map(p => (p.id, p.x, p.y)).toDF("id","x","y")

