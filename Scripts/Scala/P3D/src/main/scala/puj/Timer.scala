package puj

import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.scala.Logging

object Timer extends Logging {
  def time[T](msg: String = "")(block: => T): T = {
    val start = System.nanoTime()
    val result = block // Execute the code block here
    val end = System.nanoTime()
    
    val durationMs = (end - start) / 1e6
    logger.info(f"TIME|$msg|$durationMs%.2f ms")
    
    result // Return the result so the flow continues
  }
}
