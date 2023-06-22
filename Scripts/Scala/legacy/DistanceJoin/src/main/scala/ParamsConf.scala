package edu.ucr.dblab.djoin

import org.rogach.scallop.ScallopConf

class PairsFinderConf(args: Seq[String]) extends ScallopConf(args) {
  val points     = opt[String](default = Some(""))
  val method     = opt[String](default = Some("Partition"))
  val epsilon    = opt[Double](default = Some(10.0))
  val precision  = opt[Double](default = Some(0.001))
  val partitions = opt[Int](default = Some(256))
  val cores      = opt[Int](default = Some(1))
  val capacity   = opt[Int](default = Some(20))
  val fraction   = opt[Double](default = Some(0.01))
  val levels     = opt[Int](default = Some(5))
  val threshold  = opt[Int](default = Some(5000))
  val debug      = opt[Boolean](default = Some(false))

  verify()
}

class DiskFinderConf(args: Seq[String]) extends ScallopConf(args) {
  val points      = opt[String](default = Some(""))
  val epsilon     = opt[Double](default = Some(10.0))
  val mu          = opt[Int](default = Some(2))
  val precision   = opt[Double](default = Some(0.001))
  val threshold   = opt[Int](default = Some(5000))
  val capacity    = opt[Int](default = Some(20))
  val fraction    = opt[Double](default = Some(0.01))
  val levels      = opt[Int](default = Some(5))
  val partitions  = opt[Int](default = Some(256))
  val factor      = opt[Int](default = Some(4))
  val method      = opt[String](default = Some("Partition"))
  val persistance = opt[Int](default = Some(1))
  val debug       = opt[Boolean](default = Some(false))

  verify()
}

class FlockFinderConf(args: Seq[String]) extends ScallopConf(args) {
  val points     = opt[String](default = Some(""))
  val epsilon    = opt[Double](default = Some(10.0))
  val mu         = opt[Int](default = Some(2))
  val delta      = opt[Int](default = Some(3))
  val precision  = opt[Double](default = Some(0.001))
  val threshold  = opt[Int](default = Some(5000))
  val capacity   = opt[Int](default = Some(20))
  val fraction   = opt[Double](default = Some(0.01))
  val levels     = opt[Int](default = Some(5))
  val partitions = opt[Int](default = Some(256))
  val lparts     = opt[Int](default = Some(0))
  val method     = opt[String](default = Some("None"))
  val debug      = opt[Boolean](default = Some(false))

  verify()
}

class TaskCoreMapperConf(args: Seq[String]) extends ScallopConf(args) {
  val filename = opt[String](default = Some(""))
  val cores    = opt[Int](default = Some(8))
  val maxt     = opt[Int](default = Some(500))

  verify()
}
