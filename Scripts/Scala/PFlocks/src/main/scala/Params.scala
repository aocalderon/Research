package edu.ucr.dblab.pflock

import org.rogach.scallop._

class Params(args: Seq[String]) extends ScallopConf(args) {
  val input:       ScallopOption[String]  = opt[String]  (default = Some(""))

  val bycapacity:  ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  val maxentries:  ScallopOption[Int]     = opt[Int]     (default = Some(1000))
  val fraction:    ScallopOption[Double]  = opt[Double]  (default = Some(0.01))
  val maxlevel:    ScallopOption[Int]     = opt[Int]     (default = Some(8))
  val partitions:  ScallopOption[Int]     = opt[Int]     (default = Some(1024))

  val epsilon:     ScallopOption[Double]  = opt[Double]  (default = Some(10.0))
  val mu:          ScallopOption[Int]     = opt[Int]     (default = Some(5))

  val width:       ScallopOption[Double]  = opt[Double]  (default = Some(100.0))

  val storage:     ScallopOption[Int]     = opt[Int]     (default = Some(0)) // Default value for storage level...
  val seed:        ScallopOption[Long]    = opt[Long]    (default = Some(42L))
  val tolerance:   ScallopOption[Double]  = opt[Double]  (default = Some(0.001))
  val output:      ScallopOption[String]  = opt[String]  (default = Some("/tmp"))
  val debug:       ScallopOption[Boolean] = opt[Boolean] (default = Some(false))

  verify()
}
