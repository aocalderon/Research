package edu.ucr.dblab.pflock

import org.rogach.scallop._
import java.io.PrintWriter
import sys.process._

object Runner {
  def main(args: Array[String]): Unit = {
    val params = new BFEParams(args)

    val HOME = sys.env("HOME")
    val JARS_PATH = s"$HOME/Spark/2.4/jars/"

    val JARS = List(
      "geospark-1.3.1.jar",
      "scallop_2.11-4.0.1.jar",
      "slf4j-api-1.7.25.jar",
      "archery_2.11-0.6.0.jar",
      "jgrapht-core-1.4.0.jar",
      "commons-geometry-core-1.0-beta1.jar",
      "commons-geometry-enclosing-1.0-beta1.jar",
      "commons-geometry-euclidean-1.0-beta1.jar",
      "commons-numbers-fraction-1.0-beta1.jar",
      "commons-numbers-core-1.0-beta1.jar",
      "commons-numbers-arrays-1.0-beta1.jar"
    ).map{ jar => s"${JARS_PATH}$jar" }.mkString(",")

    val MASTER = params.master()
    val LOG_FILE = s"$HOME/Spark/2.4/conf/log4j.properties"
    val CLASS = s"edu.ucr.dblab.pflock.${params.method()}"
    val JAR = s"$HOME/Research/Scripts/Scala/Cliques/target/scala-2.11/cliques_2.11-0.1.jar"

    val BASE = List(
      s"--files $LOG_FILE",
      s"--conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$LOG_FILE",
      s"--jars $JARS",
      s"--master $MASTER --deploy-mode client",
      s"--class $CLASS $JAR"
    ).mkString(" ")

    val data = for{
      n <- (1 to 5)
      epsilon <- List(3,4,5)
    } yield {
      val PARAMS = List(
        s"--input ${params.input()}",
        s"--epsilon ${epsilon}",
        s"--mu ${params.mu()}",
        s"--method ${params.method()}"
      ).mkString(" ")

      println{s"${n} spark-submit $PARAMS"}

      val o = (s"spark-submit $BASE $PARAMS" !!).split("\n").toList
      o
    }

    case class Record(method: String, epsilon: Double, stage: String, time: Double){
      override def toString: String = s"$method\t$stage\t$epsilon\t$time\n"
    }
    val tsv = data.flatten
      .filter(_.contains("TIME"))
      .map{ line =>
        val arr = line.split('|')

        val epsilon = arr(4).toDouble
        val method  = arr(7)
        val stage   = arr(8).trim()
        val time    = arr(9).toDouble

        Record(method, epsilon, stage, time)
      }
      .groupBy(r => (r.method, r.stage, r.epsilon))
      .values.map{ group =>
        val meanTime = group.map(_.time).sum / group.size.toDouble
        group.head.copy(time = meanTime)
      }.toList
      .sortBy(r => (r.method, r.stage, r.epsilon))
      .map{_.toString}

    val datafile = s"${HOME}/Research/tmp/RE${params.tag()}.tsv"
    val f = new PrintWriter(datafile)
    f.write{ "method\tstage\tepsilon\ttime\n" + tsv.mkString("") }
    f.close
    println(s"$datafile [${tsv.size} records] saved.")
  }
}
