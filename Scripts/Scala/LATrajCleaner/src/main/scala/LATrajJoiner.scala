import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import java.io.PrintWriter

object LATrajJoiner {
  private val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class ST_Point(pid: Int, x: Double, y: Double, t: Int)
  case class Traj(index: Long, tid: Int, lenght: Int, coords: Vector[String] = Vector.empty)
  case class Settings(trajs_per_time: Int, max_time: Int)
  case class TrajDB(trajs: RDD[Traj], index: Vector[Long], size: Int, settings: Settings)
  case class State(time: Int, ntrajs: Int)

  def timer[R](msg: String = "")(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    logger.info("LATI|%-30s|%6.2f".format(msg, (t1 - t0) / 1e9))
    result
  }

  def selectTrajs(trajs: RDD[Traj], m: Int, n: Int): Vector[(Int, Int)] = {
    val indices = trajs.map(_.index).collect().sorted
    val indicesCount = indices.size
    var start = 0
    var end = m
    var trajCount = trajs.count() - (end - start)

    val sample = trajs.filter{ traj =>
      val s = indices(start)
      val e = indices(end)
      traj.index >= s & traj.index < e
    }
    val output = new ArrayBuffer[(Int, Int)]()
    val out = (0, sample.count().toInt)
    output += out

    def getPointsByTimeInterval(sample: RDD[Traj], n: Int): Vector[(Int, Int)] = {
      val dataset = sample.flatMap{ traj =>
        (n until (n + traj.lenght)).map(t => (t, traj.tid))
      }
      dataset.groupBy(_._1).map(traj => (traj._1, traj._2.size)).collect()
        .map(r => (r._1, r._2)).toVector.sortBy(x => x._1)
    }

    var state = getPointsByTimeInterval(sample, 0)
    logger.info(s"${state.take(3)} ... ${state.reverse.take(3)}")
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
      state = (state ++ newState).groupBy(_._1).mapValues{ f =>
        f.map(_._2).reduce(_ + _)
      }.toVector.sortBy(_._1).filter(_._1 >= i)

      logger.info(s"${state.take(3)} ... ${state.reverse.take(3)}")
      i = i + 1
    }
    output.toVector
  }

  def loop(trajDB: TrajDB, state: Vector[State], cursor: Int): Vector[State] = {
    if(cursor > trajDB.size || state.isEmpty){
      state
    } else {
      val time = state.head.time
      if(time > trajDB.settings.max_time){
        state
      } else {
        val ntrajs = state.head.ntrajs
        logger.info(s"Evaluating time $time")
        state.takeRight(5).foreach { println }

        val trajs_needed = trajDB.settings.trajs_per_time - ntrajs
        val sample = trajDB.trajs.filter{ traj =>
          val s = trajDB.index(cursor)
          val e = trajDB.index(cursor + trajs_needed)
          traj.index >= s & traj.index < e
        }
        val new_state = (sample.flatMap{ traj =>
          (time until (time + traj.lenght)).map(t => (t, traj.tid))
        }.groupBy(_._1).map(traj => State(traj._1, traj._2.size)).collect()
          .toVector ++ state).groupBy(_.time).mapValues{ f =>
          f.reduce{ (a, b) => State(a.time, a.ntrajs + b.ntrajs) }
        }.values.toVector.sortBy(_.time)


        if(new_state.isEmpty){
          new_state
        } else {
          State(time, trajs_needed) +: loop(trajDB, new_state.tail, cursor + trajs_needed)
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val params = new LATrajJoinerConf(args)
    val input = params.input()
    val tableFile = params.table()
    val output = params.output()
    val partitions = params.partitions()
    def debug[R](block: => R): Unit = { if(params.debug()) block }

    val spark = timer{"Starting session"}{
      SparkSession.builder()
        .appName("LATrajJoiner")
        .getOrCreate()
    }
    import spark.implicits._

    val (trajs, nTrajs) = timer{"Reading trajectories"}{
      val trajs = spark.read.option("header", "false").option("delimiter", "\t").csv(input)
        .map{ row =>
          val pid = row.getString(0).toInt
          val x = row.getString(1).toDouble
          val y = row.getString(2).toDouble
          val t = row.getString(3).toInt
          ST_Point(pid, x, y, t)
        }
        .rdd.groupBy(_.pid).map{ case (tid, points) =>
            val coords = points.toVector.sortBy(_.t).map(point => s"${point.x},${point.y}")
            (tid, coords)
        }
        .zipWithUniqueId()
        .map{ case((tid, coords), id ) =>
          Traj(id, tid, coords.length, coords)
        }.cache
      val nTrajs = trajs.count()
      (trajs, nTrajs)
    }

    debug{
      logger.info(s"Trajs number of records: ${nTrajs}")
    }

    val table = timer{"Testing recursion"}{
      val settings = Settings(params.ntrajs(), params.ntime())
      val index = trajs.map(_.index).collect().toVector.sorted
      val size = index.size
      val trajDB = TrajDB(trajs, index, size, settings)
      val state = Vector(State(0, 0))
      val cursor = 0
      loop(trajDB, state, cursor)
    }

    debug{
      table.map(state => (state.time, state.ntrajs)).toDF("Time", "N").show
    }

    /*
    val index = timer{"Reading table"}{
      val index = spark.read.option("header", "false").option("delimiter", "\t").csv(tableFile)
      .map{ index =>
        val t = index.getString(0).toInt
        val pid = index.getString(1).toInt
        (pid, t)
      }.cache()
      val nIndex = index.count()
      index
    }

    val data = timer{"Join trajectories and table"}{
      val data = index.toDF("pid1","t").join(trajs.toDF("pid2","points"), $"pid1" === $"pid2")
      .flatMap{ point =>
        val pid = point.getInt(0)
        val t = point.getInt(1)
        val points = point.getList[String](3).asScala
          .map{ p => (p.split(",")(0).toDouble, p.split(",")(1).toDouble)}
        val n = points.size
        val pids = List.fill(n)(pid)
        val ts = (t until (t + n))
        pids.zip(ts).zip(points).map(p => ST_Point(p._1._1, p._2._1, p._2._2, p._1._2))
      }
      .cache()
      val nData = data.count()
      data
    }

    timer{"Saving dataset"}{
      val f = new java.io.PrintWriter(output)
      f.write(
        data.orderBy($"t").collect()
          .map{ p =>
            s"${p.pid}\t${p.x}\t${p.y}\t${p.t}\n"
          }.mkString("")
      )
      f.close()
    }
     */

    timer{"Closing session"}{
      spark.close()
    }
  }
}

class LATrajJoinerConf(args: Seq[String]) extends ScallopConf(args) {
  val input: ScallopOption[String] = opt[String] (required = true)
  val table: ScallopOption[String] = opt[String] (required = true)
  val output: ScallopOption[String] = opt[String] (default  = Some("/tmp/output.tsv"))
  val ntrajs: ScallopOption[Int] = opt[Int] (required = true)
  val ntime: ScallopOption[Int] = opt[Int] (default = Some(10))
  val partitions: ScallopOption[Int] = opt[Int] (default = Some(256))
  val debug: ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
