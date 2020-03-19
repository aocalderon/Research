package edu.ucr.dblab

import org.slf4j.{Logger, LoggerFactory}

object Utils {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def clocktime: Long = System.nanoTime()

  def timer[R](msg: String)(block: => R)(implicit logger: Logger): R = {
    val t0 = clocktime
    val result = block    // call-by-name
    val t1 = clocktime
    logger.info("%-30s|%6.2f".format(msg, (t1 - t0) / 1e9))
    result
  }

  def debug[R](block: => R)(implicit d: Boolean): Unit = { if(d) block }

  def save(filename: String)(content: Seq[String])(implicit logger: Logger): Unit = {
    val start = clocktime
    val f = new java.io.PrintWriter(filename)
    f.write(content.mkString(""))
    f.close
    val end = clocktime
    val time = "%.2f".format((end - start) / 1e9)
    logger.info(s"Saved ${filename} in ${time}s [${content.size} records].")
  }

  def main(args: Array[String]): Unit = {
    implicit val b: Boolean = true

    timer{"Testing"}{
      logger.info("timer")
    }

    debug{
      logger.info("debug")
    }

    val v = timer{"Another test"}{
      val v = Vector(1,2,3,4)

      val fold = v.foldLeft(1)(_ + _)

      logger.info(s"$fold")

      v
    }

    save{"/tmp/output.txt"}{
      v.map(_.toString)
    }
  }
}
