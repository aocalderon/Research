import scala.collection.mutable.ListBuffer

object InfoTracker{
  var master: String = ""
  var port: String = ""
  var applicationID: String = ""
  var executors: Int = 0
  var cores: Int = 0

  def getExectutorsInfo(): String = {
    var logger = new ListBuffer[String]()
    try{
      val url = s"http://${master}:${port}/api/v1/applications/${applicationID}/executors"
      val r = requests.get(url)
      if(s"${r.statusCode}" == "200"){
        import scala.util.parsing.json._
        val j = JSON.parseFull(r.text).get.asInstanceOf[List[Map[String, Any]]]
        j.filter(_.get("id").get != "driver").foreach{ e =>
          val eid    = e.get("id").get
          val ehost  = e.get("hostPort").get
          val ecores = "%.0f".format(e.get("totalCores").get)
          val erdds  = "%.0f".format(e.get("rddBlocks").get)
          val etasks = "%.0f".format(e.get("totalTasks").get)
          val etime  = "%.2fs".format(e.get("totalDuration").get.asInstanceOf[Double] / (e.get("totalCores").get.asInstanceOf[Double] * 1000.0))
          val einput = "%.2fMB".format(e.get("totalInputBytes").get.asInstanceOf[Double] / (1024.0 * 1024))
          logger += s"EXECUTORS|$eid|$executors|$ecores|$erdds|$etasks|$etime|$einput|$ehost|$applicationID\n"
        }
      }
      logger.toList.mkString("")
    } catch {
      case e: java.net.ConnectException => s"No executors information... ${e.getMessage}"
    }
  }

  def getStagesInfo(): String = {
    var logger = new ListBuffer[String]()
    try{
      val url = s"http://${master}:${port}/api/v1/applications/${applicationID}/stages"
      val r = requests.get(url)
      if(s"${r.statusCode}" == "200"){
        import scala.util.parsing.json._
        val j = JSON.parseFull(r.text).get.asInstanceOf[List[Map[String, Any]]]
        j.filter(_.get("status").get == "COMPLETE").foreach{ s =>
          val sid    = "%4.0f".format(s.get("stageId").get)
          val sname  = "%-37s".format(s.get("name").get.toString())
          val submissionTime     = "%s".format(s.get("submissionTime").get)
          val completionTime     = "%s".format(s.get("completionTime").get)
          val numTasks           = "%4.0f".format(s.get("numTasks").get)
          val executorRunTime    = "%.0f".format(s.get("executorRunTime").get)
          val executorCpuTime    = "%.0f".format(s.get("executorCpuTime").get)
          val inputBytes         = "%.0f".format(s.get("inputBytes").get)
          val inputRecords       = "%.0f".format(s.get("inputRecords").get)
          val shuffleReadBytes   = "%.0f".format(s.get("shuffleReadBytes").get)
          val shuffleReadRecords = "%.0f".format(s.get("shuffleReadRecords").get)
          logger += s"STAGES|$sid|$sname|$executors|$cores|$submissionTime|$completionTime|$numTasks|$executorRunTime|$executorCpuTime|$inputBytes|$inputRecords|$shuffleReadBytes|$shuffleReadRecords|$applicationID\n"
        }
      }
      logger.toList.mkString("")
    } catch {
      case e: java.net.ConnectException => s"No stages information... ${e.getMessage}"
    }
  }

  def getTasksInfo(): String = {
    var logger = new ListBuffer[String]()
    try{
      val url1 = s"http://${master}:${port}/api/v1/applications/${applicationID}/stages"
      val r = requests.get(url1)
      if(s"${r.statusCode}" == "200"){
        import scala.util.parsing.json._
        val j = JSON.parseFull(r.text).get.asInstanceOf[List[Map[String, Any]]]
        j.filter(_.get("status").get == "COMPLETE").foreach{ s =>
          val stageId ="%.0f".format(s.get("stageId").get)
          val sid    = "%4.0f".format(s.get("stageId").get)
          val sname  = "%-37s".format(s.get("name").get.toString())
          val ntasks = "%5.0f".format(s.get("numTasks").get)
          val url2 = s"http://${master}:${port}/api/v1/applications/${applicationID}/stages/${stageId}"
          val u = requests.get(url2)
          val k = JSON.parseFull(u.text).get.asInstanceOf[List[Map[String, Any]]]
          k.foreach { v  =>
            val tasks = v.get("tasks").get.asInstanceOf[Map[String, Map[String, Any]]]
            tasks.values.filter(_.get("status").get == "SUCCESS").foreach { t =>
              val tasksId      = "%7.0f".format(t.get("taskId").get)
              val duration     = "%4.0f".format(t.get("duration").get)
              val launchTime   = "%s".format(t.get("launchTime").get.toString())
              val host         = "%s".format(t.get("host").get.toString())
              val taskLocality = "%s".format(t.get("taskLocality").get.toString())
              val taskMetrics  = t.get("taskMetrics").get.asInstanceOf[Map[String, Any]]
              val executorRunTime = "%.0f".format(taskMetrics.get("executorRunTime").get)
              val resultSize   = "%.0f".format(taskMetrics.get("resultSize").get)
              val inputMetrics   = taskMetrics.get("inputMetrics").get.asInstanceOf[Map[String, Any]]
              val sInputMetrics  = "%.0f|%.0f".format(inputMetrics.get("bytesRead").get, inputMetrics.get("recordsRead").get)
              val outputMetrics  = taskMetrics.get("outputMetrics").get.asInstanceOf[Map[String, Any]]
              val sOutputMetrics = "%.0f|%.0f".format(outputMetrics.get("bytesWritten").get, outputMetrics.get("recordsWritten").get)

              val shuffleReadMetrics   = taskMetrics.get("shuffleReadMetrics").get.asInstanceOf[Map[String, Any]]
              val sShuffleReadMetrics  = "%.0f|%.0f".format(shuffleReadMetrics.get("remoteBytesRead").get, shuffleReadMetrics.get("recordsRead").get)
              val shuffleWriteMetrics  = taskMetrics.get("outputMetrics").get.asInstanceOf[Map[String, Any]]
              val sShuffleWriteMetrics = "%.0f|%.0f".format(shuffleWriteMetrics.get("bytesWritten").get, shuffleWriteMetrics.get("recordsWritten").get)

              logger += s"TASKS|$sid|$sname|$tasksId|$executors|$cores|$ntasks|$duration|$launchTime|$host|$taskLocality|$executorRunTime|$resultSize|$sInputMetrics|$sOutputMetrics|$sShuffleReadMetrics|$sShuffleWriteMetrics|$applicationID\n"
            }
          }
        }
      }
      logger.toList.mkString("")
    } catch {
      case e1: java.net.ConnectException        => s"No tasks information... ${e1.getMessage}"
      case e2: java.util.NoSuchElementException => s"No tasks information...  ${e2.getMessage}"
    }
  }
}
