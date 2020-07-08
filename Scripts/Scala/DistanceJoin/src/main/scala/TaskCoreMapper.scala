package edu.ucr.dblab.djoin

import org.slf4j.{LoggerFactory, Logger}
import scala.collection.JavaConverters._
import scala.io.Source
import java.io.FileWriter

object TaskCoreMapper{
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class Task(taskId: Int, host: String, launch: Long, finish: Long, duration: Int){
    def getLaunch: Int = launch.toInt
    def getFinish: Int = finish.toInt

    override def toString(): String = s"$host\t$taskId\t$launch\t$finish\t$duration\n"
  }

  case class Matrix(cores: Int, times: Int){
    val T = (0 to cores).map(n => Array.fill(times){0}).toVector
    val empty = '-'
    val busy  = '*'

    def set(core: Int, time: Int, taskId: Int): Unit = T(core)(time) = taskId

    def get(core: Int, time: Int): Int = T(core)(time)

    def update(core: Int, u: Array[Int], start: Int, duration: Int): Unit = {
      u.copyToArray(T(core), start, duration)
    }

    def getAvailableCore(time: Int): Int = {
      val candidates = (0 to cores).map(core => T(core)(time))
      val core = candidates.indexOf(0)
      if(core >= 0){
        core
      } else {
        getAvailableCore(time + 1)
      }
    }

    override def toString(): String = {
      (0 to cores).map{ core =>
        s"$core : " +: T(core).map{x => if(x == 0) empty else busy}.mkString("")
      }.mkString("\n")
    }
  }

  def main(args: Array[String]): Unit = {
    implicit val params = new TaskCoreMapperConf(args)
    val filename = params.filename()
    val cores = params.cores()
    val maxt = params.maxt()

    logger.info("Reading file...")
    val bufferedSource = Source.fromFile(filename)
    val tasks_prime = bufferedSource.getLines.drop(1).map{ line =>
      val arr = line.split("\t")
      val taskId   = arr(3).toInt
      val launch   = arr(4).toLong
      val finish   = arr(5).toLong
      val duration = arr(6).toInt
      val host     = arr(9)

      Task(taskId, host, launch, finish, duration)
    }.toVector

    logger.info("Setting start and end...")
    val start = tasks_prime.map(_.launch).min
    val tasks = tasks_prime.map{ t =>
      t.copy(launch = t.launch - start, finish = t.finish - start)
    }//.filter(_.host == "mr-01")
    val end = tasks.map(_.finish).max

    val times = end.toInt + 1
    val matrix = Matrix(cores, times)
    println(times)

    val f = new FileWriter("/tmp/tcmapper.tsv")
    f.write("coreId\thost\ttaskId\tlaunchTime\tfinishTime\tduration\n")
    for(task <- tasks){
      val core = matrix.getAvailableCore(task.getLaunch)
      f.write(s"${core}\t${task.toString}")
      val interval = Array.fill(task.duration){task.taskId}
      matrix.update(core, interval, task.getLaunch, task.duration)
    }
    f.close

    logger.info("Done!")
    bufferedSource.close
  }
}

