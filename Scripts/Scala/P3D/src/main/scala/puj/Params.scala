package puj

import puj.Utils.Settings
import org.rogach.scallop._

object Setup{
  def getSettings(args: Seq[String]): Settings = { 
    val params = new Params(args)
    Settings(
      dataset = params.input(),
      tag = params.tag(),
      output = params.output(),
      method = params.method(),
      master = params.master(),
      eprime = params.epsilon(),
      sdist = params.sdist(),
      fraction = params.fraction(),
      tolerance = params.tolerance(),
      mu = params.mu(),
      delta = params.delta(),
      step = params.step(),
      endtime = params.endtime(),
      scapacity = params.scapacity(),
      tcapacity = params.tcapacity() ,
      debug = params.debug(),
      print = params.print(),
      cached = params.cached(),
      saves = params.saves()
    )
  }
}

class Params(args: Seq[String]) extends ScallopConf(args) {
  val filename = "/opt/Research/Datasets/gaussian/P25K.wkt"

  val input:  ScallopOption[String] 	= opt[String](default = Some(filename))
  val master: ScallopOption[String] 	= opt[String](default = Some("local[3]"))
  val tag:    ScallopOption[String] 	= opt[String](default = Some(""))
  val method: ScallopOption[String] 	= opt[String](default = Some("PSI"))
  val output: ScallopOption[String] 	= opt[String](default = Some("/tmp/"))
  val epsilon:   ScallopOption[Double] 	= opt[Double](default = Some(10.0))
  val sdist:     ScallopOption[Double] 	= opt[Double](default = Some(20.0))
  val fraction:  ScallopOption[Double] 	= opt[Double](default = Some(0.1))
  val tolerance: ScallopOption[Double] 	= opt[Double](default = Some(1e-3))
  val mu:        ScallopOption[Int] 	= opt[Int](default = Some(3))
  val delta:     ScallopOption[Int] 	= opt[Int](default = Some(3))
  val step:      ScallopOption[Int] 	= opt[Int](default = Some(1))
  val endtime:   ScallopOption[Int] 	= opt[Int](default = Some(10))
  val scapacity: ScallopOption[Int] 	= opt[Int](default = Some(200))
  val tcapacity: ScallopOption[Int] 	= opt[Int](default = Some(200))
  val debug:  ScallopOption[Boolean] 	= opt[Boolean](short = 'd', default = Some(false), descr = "Enable debug mode")
  val print:  ScallopOption[Boolean] 	= opt[Boolean](short = 'p', default = Some(false), descr = "Enable print mode")
  val cached: ScallopOption[Boolean] 	= opt[Boolean](short = 'c', default = Some(false), descr = "Enable cached mode")
  val saves:  ScallopOption[Boolean] 	= opt[Boolean](short = 's', default = Some(false), descr = "Enable saves mode")

  verify()
  
  def getSettings(args: Seq[String]): Settings = {
    val params = new Params(args)
    Settings(
      dataset = params.input(),
      tag = params.tag(),
      output = params.output(),
      method = params.method(),
      master = params.master(),
      eprime = params.epsilon(),
      sdist = params.sdist(),
      fraction = params.fraction(),
      tolerance = params.tolerance(),
      mu = params.mu(),
      delta = params.delta(),
      step = params.step(),
      endtime = params.endtime(),
      scapacity = params.scapacity(),
      tcapacity = params.tcapacity() ,
      debug = params.debug(),
      print = params.print(),
      cached = params.cached(),
      saves = params.saves()
    )
  }
}
