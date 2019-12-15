package org.andress

import org.slf4j.{Logger, LoggerFactory}

object Utils {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def debug[R](block: => R)(implicit d: Boolean): Unit = { if(d) block }

  def timer[R](msg: String)(block: => R)(implicit logger: Logger): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    logger.info("%-30s|%6.2f".format(msg, (t1 - t0) / 1e9))
    result
  }

  def main(args: Array[String]): Unit = {
    implicit val b: Boolean = true

    timer{"Testing"}{
      logger.info("timer")
    }

    debug{
      logger.info("debug")
    }
  }
}
