package edu.ucr.dblab.pflock

import org.rogach.scallop._

class Params(args: Seq[String]) extends ScallopConf(args) {
  val scale:   ScallopOption[Double]  = opt[Double]  (default = Some(1000.0))
  val input:   ScallopOption[String]  = opt[String]  (default = Some(""))
  val epsilon: ScallopOption[Double]  = opt[Double]  (default = Some(10.0))
  val mu:      ScallopOption[Int]     = opt[Int]     (default = Some(5))
  val output:  ScallopOption[String]  = opt[String]  (default = Some("/tmp"))
  val debug:   ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
