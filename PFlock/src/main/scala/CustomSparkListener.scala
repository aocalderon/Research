import org.apache.spark.scheduler.{SparkListener,
  SparkListenerApplicationStart, SparkListenerApplicationEnd,
  SparkListenerJobStart, SparkListenerJobEnd,
  SparkListenerStageSubmitted, SparkListenerStageCompleted,
  SparkListenerTaskStart, SparkListenerTaskEnd}
import org.slf4j.{Logger, LoggerFactory}
import java.sql.Timestamp

class CustomSparkListener extends SparkListener {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
  var appId = ""
  var jobId = 0
  var nstages = 0
  var stageId = 0
  var stageName = ""
  var ntasks = 0

  override def onApplicationStart(app: SparkListenerApplicationStart): Unit = {
    appId = app.appId.get.split("-").last
    logger.info(s"APP|START|$appId|${app.time}")
  }

  override def onApplicationEnd(app: SparkListenerApplicationEnd): Unit = {
    logger.info(s"APP|END|$appId|${app.time}")
  }

  override def onJobStart(job: SparkListenerJobStart) {
    jobId = job.jobId
    nstages = job.stageInfos.size
    logger.info(s"JOB|START|$jobId|$nstages")
  }
  override def onJobEnd(job: SparkListenerJobEnd) {
    logger.info(s"JOB|END|$jobId|$nstages")
  }

  override def onStageSubmitted(stage: SparkListenerStageSubmitted): Unit = {
    stageId = stage.stageInfo.stageId
    stageName = stage.stageInfo.name
    ntasks = stage.stageInfo.numTasks
    logger.info(s"STAGE|START|$stageName|$ntasks")
  }
  override def onStageCompleted(stage: SparkListenerStageCompleted): Unit = {
    logger.info(s"STAGE|END|$stageName")
  }

  override def onTaskEnd(task: SparkListenerTaskEnd): Unit = {
    val t = task.taskInfo
    val m = task.taskMetrics
    val info = s"TASKINFO|$stageName|" +
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
    s"$stageId|" +
    s"$jobId|" +
    s"$appId"
    logger.info(info)
  }
}
