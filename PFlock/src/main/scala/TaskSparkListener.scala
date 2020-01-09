import org.apache.spark.scheduler.{SparkListener,
  SparkListenerApplicationStart, SparkListenerApplicationEnd,
  SparkListenerStageSubmitted, SparkListenerStageCompleted,
  SparkListenerTaskStart, SparkListenerTaskEnd}
import org.slf4j.{Logger, LoggerFactory}

class TaskSparkListener extends SparkListener {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class Stage(stageId: Int, name: String, ntasks: Int, duration: Double, details: String, jobId: Long, appId: String)

  var stage = Stage(0,"",0,0,"",0,"")

  override def onApplicationStart(app: SparkListenerApplicationStart): Unit = {
    val appId = app.appId.get.split("-").last
    stage = stage.copy(appId = appId)
  }

  override def onStageSubmitted(submitted: SparkListenerStageSubmitted): Unit = {
    val s = submitted.stageInfo
    val d = s.details.split("\n").filter{ line =>
      line.contains("MF.scala") || line.contains("FF.scala") || line.contains("FE.scala")
    }.distinct.take(3).mkString(" ")
    stage = stage.copy(stageId = s.stageId, name = s.name, ntasks = s.numTasks, details = d)
  }
  override def onStageCompleted(completed: SparkListenerStageCompleted): Unit = {
    val s = completed.stageInfo
    stage = stage.copy(duration = (s.completionTime.get - s.submissionTime.get) / 1000.0)
  }

  override def onTaskEnd(task: SparkListenerTaskEnd): Unit = {
    val t = task.taskInfo
    val m = task.taskMetrics
    val info = s"TASKINFO|${stage.name}|" +
    s"${stage.ntasks}|" +
    s"${stage.stageId}|" +
    s"${t.taskId}|" +
    s"${t.index}|" +
    s"${t.host}:${t.executorId}|" +
    s"${t.duration}|" +
    s"${m.inputMetrics.recordsRead}|" +
    s"${m.inputMetrics.bytesRead}|" +
    s"${m.outputMetrics.recordsWritten}|" +
    s"${m.outputMetrics.bytesWritten}|" +
    s"${m.shuffleReadMetrics.recordsRead}|" +
    s"${m.shuffleWriteMetrics.recordsWritten}|" +
    s"${stage.jobId}|" +
    s"${stage.appId}|" +
    s"${stage.details}"
    logger.info(info)
  }
}
