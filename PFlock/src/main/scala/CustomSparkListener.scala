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
    ntasks = stage.stageInfo.numTasks
    logger.info(s"STAGE|START|$stageId|$ntasks")
  }
  override def onStageCompleted(stage: SparkListenerStageCompleted): Unit = {
    logger.info(s"STAGE|END|$stageId|$ntasks")
  }

  override def onTaskEnd(task: SparkListenerTaskEnd): Unit = {
    val t = task.taskInfo
    val m = task.taskMetrics
    val info = s"TASKINFO|" +
    s"taskId=${t.taskId}|" +
    s"index=${t.index}|" +
    s"executor=${t.host}:${t.executorId}|" +
    s"duration=${t.duration}|" +
    s"recordsRead=${m.inputMetrics.recordsRead}|" +
    s"bytesRead=${m.inputMetrics.bytesRead}|" +
    s"stageId=$stageId|" +
    s"jobId=$jobId|" +
    s"appId=$appId"
    logger.info(info)
  }
}
