import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import java.io.PrintWriter
import scala.annotation.tailrec
import org.andress.Utils.{debug, timer}

object TrajSpreader {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class ST_Point(pid: Int, x: Double, y: Double, t: Int)
  case class Traj(index: Long, tid: Int, lenght: Int, coords: Vector[String] = Vector.empty)
  case class Settings(trajs_per_time: Int, max_time: Int)
  case class TrajDB(trajs: RDD[Traj], index: Vector[Long], size: Int, settings: Settings)
  case class State(time: Int, ntrajs: Int)

  @tailrec
  def getTable(trajDB: TrajDB, state: Vector[State], cursor: Int, table: Vector[State]): Vector[State] = {
    if(cursor > trajDB.size || state.isEmpty){
      table
    } else {
      val time = state.head.time
      if(time > trajDB.settings.max_time){
        table
      } else {
        val ntrajs = state.head.ntrajs
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

        val shead = new_state.take(3).map(s => s"${s.time}: ${s.ntrajs}").mkString(", ")
        val stail = new_state.takeRight(3).map(s => s"${s.time}: ${s.ntrajs}").mkString(", ")
        logger.info(s"Evaluating time $time => $shead ... $stail")

        val new_table = table :+ State(time, trajs_needed)
        getTable(trajDB, new_state.tail, cursor + trajs_needed, new_table)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val params = new TrajSpreaderConf(args)
    val input = params.input()
    val output = params.output()
    implicit val d = params.debug()

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
            val coords = points.toVector.sortBy(_.t).map(point => s"${point.x}\t${point.y}")
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

    val (table, trajDB) = timer{"Getting time and trajs table"}{
      val settings = Settings(params.tpt(), params.maxtime())
      val index = trajs.map(_.index).collect().toVector.sorted
      val size = index.size
      val trajDB = TrajDB(trajs, index, size, settings)
      val initial_state = Vector(State(0, 0))
      val cursor = 0
      val table = getTable(trajDB, initial_state, cursor, Vector.empty[State])
      (table, trajDB)
    }

    debug{
      table.map(state => (state.time, state.ntrajs)).toDF("Time", "N").show
    }

    
    val index = timer{"Gettng index"}{
      val sums = table.scanLeft(State(0, 0))((a, b) => State(b.time, a.ntrajs + b.ntrajs))
      val bounds = sums.zip(sums.tail).map{ case(s1, s2) => (s2.time, s1.ntrajs, s2.ntrajs - 1) }
      val index = bounds.flatMap{ case (time, start, end) =>
        (start until end).map(i => (trajDB.index(i), time))
      }
      val indexDS = spark.createDataset(index).cache()
      indexDS.count()
      indexDS
    }

    debug{
      index.show()
    }

    val (data, nData) = timer{"Join trajectories and index"}{
      val left  = index.toDF("id1","t")
      val right = trajs.toDF("id2","pid","n","coords") 
      val data = left.join(right, $"id1" === $"id2")
        .flatMap{ point =>
          val t = point.getInt(1)
          val pid = point.getInt(3)
          val n = point.getInt(4)
          val coords = point.getList[String](5).asScala
          val pids = List.fill(n)(pid)
          val ts = (t until (t + n))
          pids.zip(ts).zip(coords)
            .map{ case( (pid, t), coord ) => (pid, coord, t) }
        }.rdd.cache()
      val nData = data.count()
      (data, nData)
    }

    debug{
      logger.info(s"Number of records in new dataset: $nData")
      data.take(10).foreach{println}
    }

    timer{"Saving dataset"}{
      val f = new java.io.PrintWriter(output)
      f.write(
        data.sortBy(_._3)
          .map{ case (pid, coord, t) => s"$pid\t$coord\t$t\n"}
          .collect().mkString("")
      )
      f.close()
    }

    timer{"Closing session"}{
      spark.close()
    }
  }
}

class TrajSpreaderConf(args: Seq[String]) extends ScallopConf(args) {
  val input: ScallopOption[String] = opt[String] (required = true)
  val output: ScallopOption[String] = opt[String] (default  = Some("/tmp/output.tsv"))
  val tpt: ScallopOption[Int] = opt[Int] (required = true)
  val maxtime: ScallopOption[Int] = opt[Int] (default = Some(10))
  val debug: ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
