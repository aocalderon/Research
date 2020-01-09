import org.apache.spark.scheduler.{SparkListener,
  SparkListenerApplicationStart, SparkListenerApplicationEnd,
  SparkListenerJobStart, SparkListenerJobEnd,
  SparkListenerStageSubmitted, SparkListenerStageCompleted,
  SparkListenerTaskStart, SparkListenerTaskEnd}
import org.slf4j.{Logger, LoggerFactory}
import java.sql.Timestamp

class StageSparkListener extends SparkListener {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
  var appId = ""
  var jobId = 0

  override def onApplicationStart(app: SparkListenerApplicationStart): Unit = {
    appId = app.appId.get.split("-").last
    logger.info(s"APP|START|$appId|${app.time}")
  }

  override def onApplicationEnd(app: SparkListenerApplicationEnd): Unit = {
    logger.info(s"APP|END|${app.time}")
  }

  override def onJobStart(job: SparkListenerJobStart) {
    jobId = job.jobId
    val nstages = job.stageInfos.size
    logger.info(s"JOB|START|$jobId|$nstages")
  }
  override def onJobEnd(job: SparkListenerJobEnd) {
    val jobId = job.jobId
    logger.info(s"JOB|END|$jobId")
  }

  override def onStageSubmitted(stage: SparkListenerStageSubmitted): Unit = {
    val stageId = stage.stageInfo.stageId
    val ntasks = stage.stageInfo.numTasks
    logger.info(s"STAGE|START|$stageId|$ntasks")
  }
  override def onStageCompleted(stage: SparkListenerStageCompleted): Unit = {
    val s = stage.stageInfo
    val info = s"STAGEINFO|" +
    s"${s.name}|" +
    s"${s.numTasks}|" +
    s"${s.completionTime.get - s.submissionTime.get}|" +
    s"${s.stageId}|" +
    s"$jobId|" +
    s"$appId"
    logger.info(info)

    val details = s.details.split("\n").filter{ line =>
      line.contains("MF.scala") || line.contains("FF.scala") || line.contains("FE.scala")
    }.distinct.mkString(" ")
    logger.info(s"STAGEDETAILS|${s.stageId}|${details}")
  }
}
