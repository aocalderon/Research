package puj

import org.rogach.scallop._
import java.time.Instant
import org.apache.logging.log4j.scala.Logging
import org.locationtech.jts.geom.{GeometryFactory, PrecisionModel}

object Setup extends Logging {
  def getSettings(args: Seq[String]): Settings = {
    val params = new Params(args)
    Settings(
      dataset = params.input(),
      tag = params.tag(),
      output = params.output(),
      method = params.method(),
      master = params.master(),
      partitioner = params.partitioner(),
      eprime = params.epsilon(),
      sdist = params.sdist(),
      fraction = params.fraction(),
      tolerance = params.tolerance(),
      mu = params.mu(),
      delta = params.delta(),
      step = params.step(),
      endtime = params.endtime(),
      scapacity = params.scapacity(),
      tcapacity = params.tcapacity(),
      debug = params.debug(),
      print = params.print(),
      cached = params.cached(),
      experi = params.experi(),
      saves = params.saves()
    )
  }
}

case class Settings (
    dataset: String = "",
    tag: String = "",
    output: String = "",
    method: String = "",
    master: String = "",
    partitioner: String = "",
    eprime: Double = 0.0,
    sdist: Double = 0.0,
    fraction: Double = 0.0,
    tolerance: Double = 1e-3,
    mu: Int = 0,
    delta: Int = 0,
    step: Int = 0,
    endtime: Int = 0,
    scapacity: Int = 0,
    tcapacity: Int = 0,
    debug: Boolean = false,
    print: Boolean = false,
    cached: Boolean = false,
    experi: Boolean = false,
    saves: Boolean = false
) extends Logging {
  val scale: Double        = 1 / tolerance
  val epsilon: Double      = eprime + tolerance
  val r: Double            = (eprime / 2.0) + tolerance
  val r2: Double           = math.pow(eprime / 2.0, 2) + tolerance
  val expansion: Double    = eprime * 1.5 + tolerance
  var partitions: Int      = 1
  var appId: String        = s"${Instant.now()}"
  val geofactory: GeometryFactory = new GeometryFactory(new PrecisionModel(scale))
  val dataset_name: String = {
    val d = dataset.split("/").last.split("\\.").head
    if (d.startsWith("part-")) d.split("-")(1) else d
  }

  def printer: Unit = {
    logger.info(s"${appId}|SETTINGS|DATASET=$dataset")
    logger.info(s"${appId}|SETTINGS|MASTER=$master")
    logger.info(s"${appId}|SETTINGS|EPSILON=$epsilon")
    logger.info(s"${appId}|SETTINGS|MU=$mu")
    logger.info(s"${appId}|SETTINGS|DELTA=$delta")
    logger.info(s"${appId}|SETTINGS|METHOD=$method")
    logger.info(s"${appId}|SETTINGS|PARTITIONER=$partitioner")
    logger.info(s"${appId}|SETTINGS|SCAPACITY=$scapacity")
    logger.info(s"${appId}|SETTINGS|TCAPACITY=$tcapacity")
    logger.info(s"${appId}|SETTINGS|TOLERANCE=$tolerance")
    logger.info(s"${appId}|SETTINGS|STEP=$step")
    logger.info(s"${appId}|SETTINGS|SDIST=$sdist")
    logger.info(s"${appId}|SETTINGS|DEBUG=$debug")
    logger.info(s"${appId}|SETTINGS|EXPERIMENTS=$experi")
  }
}

class Params(args: Seq[String]) extends ScallopConf(args) {
  val filename = "/home/and/MEGA/Work/data/PFlocks/BERLIN_10K.tsv"

  val input: ScallopOption[String]     = opt[String](default = Some(filename))
  val master: ScallopOption[String]    = opt[String](default = Some("local[2]"))
  val tag: ScallopOption[String]       = opt[String](default = Some(""))
  val method: ScallopOption[String]    = opt[String](default = Some("PSI"))
  val output: ScallopOption[String]    = opt[String](default = Some("/tmp/"))
  val partitioner: ScallopOption[String] = opt[String](default = Some("Fixed"))
  val epsilon: ScallopOption[Double]   = opt[Double](default = Some(10.0))
  val sdist: ScallopOption[Double]     = opt[Double](default = Some(20.0))
  val fraction: ScallopOption[Double]  = opt[Double](default = Some(0.1))
  val tolerance: ScallopOption[Double] = opt[Double](default = Some(1e-3))
  val mu: ScallopOption[Int]           = opt[Int](default = Some(3))
  val delta: ScallopOption[Int]        = opt[Int](default = Some(3))
  val step: ScallopOption[Int]         = opt[Int](default = Some(1))
  val endtime: ScallopOption[Int]      = opt[Int](default = Some(-1))
  val scapacity: ScallopOption[Int]    = opt[Int](default = Some(200))
  val tcapacity: ScallopOption[Int]    = opt[Int](default = Some(200))
  val debug: ScallopOption[Boolean]    = opt[Boolean](short = 'd', default = Some(false), descr = "Enable debug mode")
  val print: ScallopOption[Boolean]    = opt[Boolean](short = 'p', default = Some(false), descr = "Enable print mode")
  val cached: ScallopOption[Boolean]   = opt[Boolean](short = 'c', default = Some(false), descr = "Enable cached mode")
  val experi: ScallopOption[Boolean]   = opt[Boolean](short = 'e', default = Some(false), descr = "Enable experiments mode")
  val saves: ScallopOption[Boolean]    = opt[Boolean](short = 's', default = Some(false), descr = "Enable saves mode")

  verify()
}
