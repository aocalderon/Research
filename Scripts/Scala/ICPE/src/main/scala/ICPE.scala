import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geospark.enums.{FileDataSplitter, GridType, IndexType}
import org.datasyslab.geospark.spatialRDD.{SpatialRDD, PointRDD, CircleRDD}
import org.datasyslab.geospark.spatialOperator.JoinQuery
import com.vividsolutions.jts.geom.{GeometryFactory, Geometry, Coordinate, Point}
import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import dbscan._

object ICPE {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")
  private val geofactory: GeometryFactory = new GeometryFactory()
  private val precision: Double = 0.0001
  private var startApp: Long = System.currentTimeMillis()
  private var applicationID: String = "app-00000000000000-0000"

  case class Pids(t: Int, pids: List[Int])

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }

  def round3(n: Double): Double = { math.round( n * 1000) / 1000.0 }

  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    logger.info("ICPE|%6.2f|%-50s|%6.2f|%6d|%s".format((clocktime-startApp)/1000.0, msg, (clocktime-timer)/1000.0, n, status))
  }

  def main(args: Array[String]): Unit = {
    val params = new ICPEConf(args)
    val debug = params.debug()
    val input = params.input()
    val output = params.output()
    val cores = params.cores()
    val executors = params.executors()
    val master = params.local() match {
      case true  => s"local[${cores}]"
      case false => s"spark://${params.host()}:${params.port()}"
    }
    val epsilon = params.epsilon()
    val mu = params.mu()
    val minpts = params.minpts()
    val width = params.width()

    var timer = clocktime
    var stage = "Session start"
    log(stage, timer, 0, "START")
    val spark = SparkSession.builder()
      .config("spark.default.parallelism", 3 * cores * executors)
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .config("spark.scheduler.mode", "FAIR")
      //.config("spark.cores.max", cores * executors)
      //.config("spark.executor.cores", cores)
      //.master(master)
      .appName("ICPE")
      .getOrCreate()
    import spark.implicits._
    startApp = spark.sparkContext.startTime
    applicationID = spark.sparkContext.applicationId
    log(stage, timer, 0, "END")

    timer = clocktime
    stage = "Data read"
    log(stage, timer, 0, "START")
    val locations = spark.read
      .option("header", false)
      .option("delimiter", "\t")
      .csv(input).map{ row =>
        val id = row.getString(0).toInt
        val x  = round3(row.getString(1).toDouble)
        val y  = round3(row.getString(2).toDouble)
        val t  = row.getString(3).toInt

        ST_Point(id, x, y, t)
      }.cache()
    val nLocations = locations.count()
    log(stage, timer, nLocations, "END")

    if(debug){
      locations.show(truncate=false)
      logger.info(s"Locations number of partitions: ${locations.rdd.getNumPartitions}")
    }

    val startTime = clocktime
    timer = clocktime
    stage = "Grid allocate"
    log(stage, timer, 0, "START")
    val gridObjects = GRIndex.allocateGrid(spark, locations, width, epsilon).cache()
    val nGridObjects = gridObjects.count
    log(stage, timer, nGridObjects, "END")

    if(debug){
      gridObjects.show(nGridObjects.toInt, truncate=false)
      logger.info(s"Grid objects number of partitions: ${gridObjects.rdd.getNumPartitions}")
    }

    timer = clocktime
    stage = "Grid query"
    log(stage, timer, 0, "START")
    val pairs = GRIndex.queryGrid(spark, gridObjects, epsilon).cache()
    val nPairs = pairs.count()
    log(stage, timer, nPairs, "END")

    if(debug){
      pairs.show(nPairs.toInt, truncate=false)
    }

    timer = clocktime
    stage = "Grid sync"
    log(stage, timer, 0, "START")
    val data = pairs.rdd.flatMap{ pair =>
      val p1 = pair._1
      val p2 = pair._2
      List(p1, p2)
    }.distinct().map{ p =>
      val x = p.x
      val y = p.y
      val da = new DoubleArray(Array(x, y))
      da.setTid(p.tid)
      da.setT(p.t)
      da
    }.collect().toList.asJava
    val nData = data.size
    log(stage, timer, nData, "END")


    timer = clocktime
    stage = "DBScan run"
    log(stage, timer, 0, "START")
    val algo = new AlgoDBSCAN()
    val clusters = algo.run(data, minpts, epsilon).asScala.toList
    val dbscanTime = (algo.getTime() / 1000.0)
    val nClusters = clusters.size
    log(stage, timer, nClusters, "END")

    if(debug){
      val hulls = clusters.zipWithIndex.map{ case (cluster, i) =>
        val coords = cluster.getVectors.asScala.toArray.map{ v =>
          new Coordinate(v.get(0), v.get(1))
        }
        val convex = new com.vividsolutions.jts.algorithm.ConvexHull(coords, geofactory)
        s"$i\t${convex.getConvexHull.toText()}\n"
      }
      val f = new java.io.PrintWriter("/tmp/hulls.wkt")
      f.write(hulls.mkString(""))
      f.close()
      logger.info(s"hulls.wkt saved [${hulls.size} records]")
      algo.printStatistics()
    }

    // Function to find centers of disks from a pair of points...
    val r  = epsilon / 2.0
    val r2 = math.pow(r, 2)
    def computeCenters(p1: ST_Point, p2: ST_Point): (Point, Point) = {
      var h = geofactory.createPoint(new Coordinate(-1.0,-1.0))
      var k = geofactory.createPoint(new Coordinate(-1.0,-1.0))
      val X: Double = p1.x - p2.x
      val Y: Double = p1.y - p2.y
      val D2: Double = math.pow(X, 2) + math.pow(Y, 2)
      if (D2 != 0.0){
        val root: Double = math.sqrt(math.abs(4.0 * (r2 / D2) - 1.0))
        val h1: Double = ((X + Y * root) / 2) + p2.x 
        val k1: Double = ((Y - X * root) / 2) + p2.y 
        val h2: Double = ((X - Y * root) / 2) + p2.x 
        val k2: Double = ((Y + X * root) / 2) + p2.y 
        h = geofactory.createPoint(new Coordinate(h1,k1))
        k = geofactory.createPoint(new Coordinate(h2,k2))
      }
      (h, k)
    }

    // Applying BFE algorithm to find maximal disks...
    timer = clocktime
    stage = "Getting maximals"
    log(stage, timer, 0, "START")
    val maximals = clusters.zipWithIndex.map{ case (cluster, i) =>
      // Getting points inside each cluster...
      val points = cluster.getVectors.asScala.toList.map{ v =>
        val p = ST_Point(v.getTid, v.get(0), v.get(1), v.getT)
        p
      }

      // Getting pairs of points which lie epsilon each other...
      val pairs = points.cross(points)
        .filter( p => p._1.tid < p._2.tid)
        .map(p => (p, p._1.distance(p._2)))
        .filter(p => p._2 <= params.epsilon())
        .map(_._1)
      // Getting centers of the candidate disks...
      val centers_prime = pairs.map{ p =>
          val p1 = p._1
          val p2 = p._2
          computeCenters(p1, p2)
        }.toList
      val centers = centers_prime.map(_._1).union(centers_prime.map(_._2))

      // Finding points inside candidate disks...
      val rtree = new com.vividsolutions.jts.index.strtree.STRtree()
      for(point <- points.map(_.getJTSPoint)){
        rtree.insert(point.getEnvelopeInternal, point)
      }
      val disks = centers.flatMap{ center =>
        rtree.query(center.buffer(r).getEnvelopeInternal).asScala.toList
          .map(_.asInstanceOf[Point])
          .filter(point => center.distance(point) <= r + precision)
          .map(point => (center, point))
          .groupBy(_._1)
          .map{ p =>
            val center = p._1 // the center
            val points = p._2.map{ p => // the points
              val point = p._2
              val pid = point.getUserData.toString().split("\t")(0).toInt
              pid
            }
            Disk(center.getX, center.getY, points.toSet)
          }
          .filter(_.count >= mu)
      }

      // Prune duplicates and redundant disks...
      val n = disks.size
      for(i <- 0 until n){
        for(j <- 0 until n){
          if(i != j){
            if(disks(i).subset != true && disks(j).subset != true){
              val pids1 = disks(i).pids
              val pids2 = disks(j).pids
            
              if(pids2.subsetOf(pids1) || pids2.equals(pids1)){
                disks(j).subset = true
              } else if(pids1.subsetOf(pids2)){
                disks(i).subset = true
              }
            }
          }
        }
      }
      val maximals = disks.filter(_.subset == false)

      (points, pairs, centers, disks, maximals)
    }.toList
    val nMaximals = maximals.flatMap(m => m._5).size
    val maximalsTime = (clocktime - timer) / 1000.0
    log(stage, timer, nMaximals, "END")
    val endTime = clocktime

    // Reporting results...
    val nP  = maximals.flatMap(m => m._1).size
    val nP2 = maximals.flatMap(m => m._2).size
    val nC  = maximals.flatMap(m => m._3).size
    val nD  = maximals.flatMap(m => m._4).size

    logger.info("ICPE  |%s|%5.1f|%2d|%6d|%6d|%6d|%6d|%6.3f|%6.3f|%6.2f|%6d".format(applicationID, epsilon, mu, nP, nP2, nC, nD, dbscanTime, maximalsTime, ((endTime - startTime) / 1000.0), nMaximals))

    if(debug){
      val filename = "points"
      val data = maximals.flatMap{ case (points, pairs, centers, disks, maximals) =>
        points.map(_.toWKT)
      }
      val f = new java.io.PrintWriter(s"/tmp/${filename}.wkt")
      f.write(data.mkString(""))
      f.close()
      logger.info(s"${filename}.wkt saved [${data.size} records]")
    }

    if(debug){
      val filename = "pairs"
      val data = maximals.flatMap{ case (points, pairs, centers, disks, maximals) =>
        pairs.map{ p =>
          s"LINESTRING(${p._1.x} ${p._1.y} , ${p._2.x} ${p._2.y})\n"
        }
      }
      val f = new java.io.PrintWriter(s"/tmp/${filename}.wkt")
      f.write(data.mkString(""))
      f.close()
      logger.info(s"${filename}.wkt saved [${data.size} records]")
    }

    if(debug){
      val filename = "centers"
      val data = maximals.flatMap{ case (points, pairs, centers, disks, maximals) =>
        centers.map{ p =>
          s"${p.toText()}\n"
        }
      }
      val f = new java.io.PrintWriter(s"/tmp/${filename}.wkt")
      f.write(data.mkString(""))
      f.close()
      logger.info(s"${filename}.wkt saved [${data.size} records]")
    }

    if(debug){
      val filename = "disks"
      val data = maximals.flatMap{ case (points, pairs, centers, disks, maximals) =>
        disks.map{ p =>
          p.toWKT
        }
      }
      val f = new java.io.PrintWriter(s"/tmp/${filename}.wkt")
      f.write(data.mkString(""))
      f.close()
      logger.info(s"${filename}.wkt saved [${data.size} records]")
    }

    if(debug){
      val filename = "maximals"
      val data = maximals.flatMap{ case (points, pairs, centers, disks, maximals) =>
        maximals.map{ p =>
          p.toWKT
        }
      }
      val f = new java.io.PrintWriter(s"/tmp/${filename}.wkt")
      f.write(data.mkString(""))
      f.close()
      logger.info(s"${filename}.wkt saved [${data.size} records]")
    }

    if(debug){
      val filename = "maximals"
      val data = maximals.flatMap{ case (points, pairs, centers, disks, maximals) =>
        maximals.map{ p =>
          s"${p.pids.toList.sorted.mkString(" ")}\n"
        }
      }.sorted
      val f = new java.io.PrintWriter(s"/tmp/${filename}.txt")
      f.write(data.mkString(""))
      f.close()
      logger.info(s"${filename}.txt saved [${data.size} records]")
    }



    timer = clocktime
    stage = "Session close"
    log(stage, timer, 0, "START")
    spark.close()
    log(stage, timer, 0, "END")
  }
}

class ICPEConf(args: Seq[String]) extends ScallopConf(args) {
  val input:      ScallopOption[String]  = opt[String]  (default = Some("/tmp/test.tsv"))
  val output:     ScallopOption[String]  = opt[String]  (default = Some("/tmp/output"))
  val host:       ScallopOption[String]  = opt[String]  (default = Some("169.235.27.138"))
  val port:       ScallopOption[String]  = opt[String]  (default = Some("7077"))
  val cores:      ScallopOption[Int]     = opt[Int]     (default = Some(4))
  val executors:  ScallopOption[Int]     = opt[Int]     (default = Some(3))
  val grid:       ScallopOption[String]  = opt[String]  (default = Some("KDBTREE"))
  val index:      ScallopOption[String]  = opt[String]  (default = Some("QUADTREE"))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(512))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val offset:     ScallopOption[Int]     = opt[Int]     (default = Some(2))
  val width:      ScallopOption[Double]  = opt[Double]  (default = Some(3.0))
  val minpts:     ScallopOption[Int]     = opt[Int]     (default = Some(2))
  val epsilon:    ScallopOption[Double]  = opt[Double]  (default = Some(1.5))
  val mu:         ScallopOption[Int]     = opt[Int]     (default = Some(5))

  verify()
}
