package Testing

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.simba.{Dataset, SimbaSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

object Tester {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  val epsilon = 0.05
  val mu = 2
  private val r2 = Math.pow(epsilon / 2.0, 2)
  val precision = 0.001
  val master = "local[*]" //"spark://169.235.27.134:7077"
  val cores = 4
  val partitions = 32

  case class ST_Point(id: Long, x: Double, y: Double, t: Int = -1)

  private var timer = System.currentTimeMillis()
  private val simba = SimbaSession.builder().master(master)
    .appName("Benchmark")
    .config("simba.index.partitions", "32")
    .getOrCreate()
  logging("Starting session", timer)

  def main(args: Array[String]): Unit = {
    import simba.implicits._

    timer = System.currentTimeMillis()
    val pointset = readingData("flocks.txt")
    val nPointset = pointset.count()
    val ids = pointset.map(_.id).collect().toList.mkString(" ")
    val Xs = pointset.map(_.x).collect().toList
    val Ys = pointset.map(_.y).collect().toList
    logging("Reading data...", timer, nPointset, "points")
    savePoints(pointset.collect().toList, "/tmp/points.txt")

    timer = System.currentTimeMillis()
    val disks = computeMaximalDisks(ids, Xs, Ys)
    val nDisks = disks.length
    logging("Computing disks...", timer, nDisks, "disks")
    disks.sorted.foreach(println)
    println(s"Number of disks: $nDisks")

    simba.stop()
  }

  private def readingData(filename: String): Dataset[ST_Point] ={
    import simba.implicits._
    import simba.simbaImplicits._

    simba.read
      .option("header", "false")
      .option("sep", "\t")
      .schema(ScalaReflection.schemaFor[ST_Point].dataType.asInstanceOf[StructType])
      .csv(filename)
      .as[ST_Point]
  }

  private def computeMaximalDisks(ids: String, Xs: List[Double], Ys: List[Double]): List[String] = {
    val points = ids.split(" ").map(_.toLong).zip(Xs.zip(Ys)).map(p => ST_Point(p._1, p._2._1, p._2._2)).toList
    val pairs = getPairs(points)
    val centers = pairs.flatMap(computeCenters)
    val disks = getDisks(points, centers)
    filterDisks(disks)
  }

  def filterDisks(input: List[List[Long]]): List[String] ={
    var ids = new collection.mutable.ListBuffer[(List[Long], Boolean)]()
    for( disk <- input.filter(_.lengthCompare(mu) >= 0) ){ ids += Tuple2(disk, true) }
    for(i <- ids.indices){
      for(j <- ids.indices){
        if(i != j & ids(i)._2){
          val ids1 = ids(i)._1
          val ids2 = ids(j)._1
          if(ids(j)._2 & ids1.forall(ids2.contains)){
            ids(i) = Tuple2(ids(i)._1, false)
          }
          if(ids(i)._2 & ids2.forall(ids1.contains)){
            ids(j) = Tuple2(ids(j)._1, false)
          }
        }
      }
    }

    ids.filter(_._2).map(_._1.sorted.mkString(" ")).toList
  }

  def getDisks(points: List[ST_Point], centers: List[ST_Point]): List[List[Long]] ={
    val PointCenter = for{ p <- points; c <- centers } yield (p, c)
    PointCenter.map(d => (d._2, d._1.id, dist( (d._1.x, d._1.y), (d._2.x, d._2.y) ))) // Getting the distance between centers and points...
      .filter(_._3 <= (epsilon / 2.0) + precision) // Filtering out those greater than epsilon / 2...
      .map(d => ( (d._1.x, d._1.y), d._2 )) // Selecting center and point ID...
      .groupBy(_._1) // Grouping by the center...
      .map(_._2.map(_._2)) // Selecting just the list of IDs...
      .toList
  }

  def getPairs(points: List[ST_Point]): List[(ST_Point, ST_Point)] ={
    val n = points.length
    var pairs = collection.mutable.ListBuffer[(ST_Point, ST_Point)]()
    for(i <- Range(0, n - 1)){
      for(j <- Range(i + 1, n)) {
        val d = dist((points(i).x, points(i).y), (points(j).x, points(j).y))
        if(d <= epsilon + precision){
          pairs += Tuple2(points(i), points(j))
        }
      }
    }
    pairs.toList
  }

  import Math.{pow, sqrt}
  def dist(p1: (Double, Double), p2: (Double, Double)): Double ={
    sqrt(pow(p1._1 - p2._1, 2) + pow(p1._2 - p2._2, 2))
  }

  def computeCenters(pair: (ST_Point, ST_Point)): List[ST_Point] = {
    var centerPair = collection.mutable.ListBuffer[ST_Point]()
    val X: Double = pair._1.x - pair._2.x
    val Y: Double = pair._1.y - pair._2.y
    val D2: Double = math.pow(X, 2) + math.pow(Y, 2)
    if (D2 != 0.0){
      val root: Double = math.sqrt(math.abs(4.0 * (r2 / D2) - 1.0))
      val h1: Double = ((X + Y * root) / 2) + pair._2.x
      val k1: Double = ((Y - X * root) / 2) + pair._2.y
      val h2: Double = ((X - Y * root) / 2) + pair._2.x
      val k2: Double = ((Y + X * root) / 2) + pair._2.y
      centerPair += ST_Point(pair._1.id, h1, k1)
      centerPair += ST_Point(pair._2.id, h2, k2)
    }
    centerPair.toList
  }

  def savePoints(points: List[ST_Point], filename: String): Unit ={
    new java.io.PrintWriter(filename) {
      write( points.map{ p => "%d,%.3f,%.3f\n".format(p.id, p.x, p.y) }.mkString("") )
      close()
    }
  }

  def logging(msg: String, timer: Long, n: Long = 0, tag: String = ""): Unit ={
    logger.info("%-50s | %6.2fs | %6d %s".format(msg, (System.currentTimeMillis() - timer)/1000.0, n, tag))
  }
}
