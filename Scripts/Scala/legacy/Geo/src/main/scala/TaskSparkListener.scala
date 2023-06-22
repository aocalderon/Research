import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationStart,
  SparkListenerJobEnd, SparkListenerJobStart,
  SparkListenerStageSubmitted, SparkListenerStageCompleted,
  SparkListenerTaskStart, SparkListenerTaskEnd}
import org.slf4j.{Logger, LoggerFactory}

class TaskSparkListener extends SparkListener {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  case class Stage(stageId: Int, name: String, ntasks: Int, duration: Double, details: String, jobId: Long, appId: String){
    override def toString: String = s"STAGEINFO|$stageId|$name|$duration|$ntasks|$jobId|$appId|$details"
  }

  case class Job(jobId: Int, start: Long, stageIds: String, appId: String)

  var stage = Stage(0,"",0,0,"",0,"")
  var job   = Job(0,0,"","")

  override def onApplicationStart(app: SparkListenerApplicationStart): Unit = {
    val appId = if(app.appId.get.contains("local")){
      app.appId.get
    } else {
      app.appId.get.takeRight(4)
    }
    job   = job.copy(appId = appId)
    stage = stage.copy(appId = appId)
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    job = job.copy(jobId = jobStart.jobId, start = jobStart.time, stageIds = jobStart.stageIds.mkString(" "))
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    logger.info(s"JOBINFO|${job.jobId}|${(jobEnd.time - job.start) / 1e3}|${job.stageIds}|${job.appId}")
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
    stage = stage.copy(duration = (s.completionTime.get - s.submissionTime.get) / 1e3)
    logger.info(stage.toString())
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
    s"${t.taskLocality}|" +
    s"${m.executorCpuTime / 1e3}|" +
    s"${m.executorRunTime / 1e3}|" +
    s"${m.executorDeserializeTime / 1e3}|" +
    s"${m.executorDeserializeCpuTime / 1e3}|" +
    s"${m.jvmGCTime / 1e3}|" +
    s"${m.resultSerializationTime / 1e3}|" +
    s"${t.duration / 1e3}|" +
    s"${(t.finishTime - t.launchTime) / 1e3}|" +
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
