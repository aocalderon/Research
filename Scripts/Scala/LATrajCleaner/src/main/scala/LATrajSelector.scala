import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import scala.collection.mutable.ArrayBuffer

object LATrajSelector {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class Traj(index: Long, tid: Int, lenght: Int, coords: Vector[String] = Vector.empty)

  def selectTrajs(trajs: RDD[Traj], m: Int, n: Int): Vector[(Int, Int)] = {
    val indices = trajs.map(_.index).collect().sorted
    val indicesCount = indices.size
    val nTrajs = trajs.count()
    var start = 0
    var end = m
    val sample = trajs.filter{ traj =>
      val s = indices(start)
      val e = indices(end)
      traj.index >= s & traj.index < e
    }
    val output = new ArrayBuffer[(Int, Int)]()
    val out = (0, sample.count().toInt)
    output += out
    var trajCount = trajs.count() - (end - start)

    def getPointsByTimeInterval(sample: RDD[Traj], n: Int): Vector[(Int, Int)] = {
      val dataset = sample.flatMap{ traj =>
        (n until (n + traj.lenght)).map(t => (t, traj.tid))
      }
      dataset.groupBy(_._1).map(traj => (traj._1, traj._2.size)).collect()
        .map(r => (r._1, r._2)).toVector.sortBy(x => x._1)
    }

    var state = getPointsByTimeInterval(sample, 0)
    logger.info(s"Current state: ${state.take(5)}")
    logger.info(s"Current state: ${state.reverse.take(5)}")
    var i = 1
    while(trajCount > 0 && i < n){
      val stage = s"Time interval: $i"
      logger.info(stage)

      val left = m - state(1)._1.toInt
      start = end
      end = end + left
      if(end >= indicesCount) {
        end = indicesCount - 1
      }
      val sample = trajs.filter{ traj =>
        val s = indices(start)
        val e = indices(end)
        traj.index >= s & traj.index < e
      }
      val out = (i, sample.count().toInt)
      output += out
      trajCount = trajCount - (end - start)
      logger.info(s"Trajs left: ${trajCount}")
      val newState = getPointsByTimeInterval(sample, i)
      state = (state ++ newState).groupBy(_._1).mapValues(f => f.map(_._2).reduce(_ + _)).toVector.sortBy(_._1).filter(_._1 >= i)

      logger.info(s"Current state: ${state.take(5)}")
      logger.info(s"Current state: ${state.reverse.take(5)}")
      i = i + 1
    }
    output.toVector
  }


  def clocktime = System.currentTimeMillis()

  def log(msg: String, timer: Long, n: Long, status: String): Unit ={
    logger.info("LATS|%-30s|%6.2f|%6d|%s".format(msg, (clocktime-timer)/1000.0, n, status))
  }

  def main(args: Array[String]): Unit = {
    val params = new LATrajSelectorConf(args)
    val input = params.input()
    val partitions = params.partitions()

    var timer = clocktime
    var stage = "Session start"
    log(stage, timer, 0, "START")
    val spark = SparkSession.builder()
      .appName("LATrajSelector")
      .getOrCreate()
    import spark.implicits._
    log(stage, timer, 0, "END")

    timer = clocktime
    stage = "Data read"
    log(stage, timer, 0, "START")
    var trajs = spark.read.option("header", "false").option("delimiter", "\t").csv(input)
      .rdd.zipWithUniqueId()
      .map{ traj =>
        val tid = traj._1.getString(0).toInt
        val length = traj._1.getString(1).toInt
        Traj(traj._2, tid, length)
      }.toDS()
    val indices = trajs.map(_.index).collect().sorted
    val indicesCount = indices.size
    val nTrajs = trajs.count()
    log(stage, timer, nTrajs, "END")

    timer = clocktime
    stage = "Initial sample"
    log(stage, timer, 0, "START")
    val m = params.m()
    var start = 0
    var end = m
    val sample = trajs.filter{ traj =>
      val s = indices(start)
      val e = indices(end)
      traj.index >= s & traj.index < e
    }
    val output = new java.io.PrintWriter(params.output())
    val out = s"0\t${sample.count()}"
    output.write(s"${out}\n")
    logger.info(out)
    var trajCount = trajs.count() - (end - start)
    val dataset = sample.map( traj => Traj(traj.tid, traj.lenght, 0))
    def getPointsByTimeInterval(trajs: Dataset[Traj], n: Int): List[(Int, Long)] = {
      val dataset = trajs.flatMap{ traj =>
        (n until (n + traj.lenght)).map(t => (traj.tid, t))
      }.toDF("tid", "t")
      dataset.groupBy($"t").count().collect().map(r => (r.getInt(0), r.getLong(1))).toList.sortBy(x => x._1)
    }
    var state = getPointsByTimeInterval(sample, 0)
    logger.info(s"Current state: ${state.take(5)}")
    logger.info(s"Current state: ${state.reverse.take(5)}")
    var n = 1
    while(trajCount > 0 && n < params.n()){
      val timer2 = clocktime
      val stage = s"Time interval: $n"
      log(stage, timer2, 0, "START")

      val left = m - state(1)._2.toInt
      start = end
      end = end + left
      if(end >= indicesCount) {
        end = indicesCount - 1
      }
      val sample = trajs.filter{ traj =>
        val s = indices(start)
        val e = indices(end)
        traj.index >= s & traj.index < e
      }
      val out = s"$n\t${sample.count()}"
      output.write(s"${out}\n")
      logger.info(out)
      trajCount = trajCount - (end - start)
      logger.info(s"Trajs left: ${trajCount}")
      val newState = getPointsByTimeInterval(sample, n)
      state = (state ++ newState).groupBy(s => s._1).mapValues(f => f.map(_._2).reduce(_ + _)).toList.sortBy(_._1).filter(_._1 >= n)

      logger.info(s"Current state: ${state.take(5)}")
      logger.info(s"Current state: ${state.reverse.take(5)}")
      n = n + 1

      log(stage, timer2, 0, "END")
    }
    output.close()
    log(stage, timer, 0, "END")

    timer = clocktime
    stage = "Session close"
    log(stage, timer, 0, "START")
    spark.close()
    log(stage, timer, 0, "END")
  }
}

class LATrajSelectorConf(args: Seq[String]) extends ScallopConf(args) {
  val input:      ScallopOption[String]  = opt[String]  (required = true)
  val output:     ScallopOption[String]  = opt[String]  (default  = Some("/tmp/output.tsv"))
  val partitions: ScallopOption[Int]     = opt[Int]     (default = Some(256))
  val local:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val m:          ScallopOption[Int]     = opt[Int]     (default = Some(50000))
  val n:          ScallopOption[Int]     = opt[Int]     (default = Some(200))

  verify()
}
