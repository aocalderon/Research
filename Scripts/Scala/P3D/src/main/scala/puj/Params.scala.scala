package puj

import org.rogach.scallop._

class Params(args: Seq[String]) extends ScallopConf(args) {
  val filename = "/opt/Research/Datasets/gaussian/P25K.wkt"

  val input: ScallopOption[String] = opt[String](default = Some(filename))
  val master: ScallopOption[String] = opt[String](default = Some("local[3]"))
  val epsilon: ScallopOption[Double] = opt[Double](default = Some(10.0))
  val mu: ScallopOption[Int] = opt[Int](default = Some(3))
  val step: ScallopOption[Int] = opt[Int](default = Some(1))
  val sdist: ScallopOption[Double] = opt[Double] (default = Some(20.0))
  val endtime: ScallopOption[Int] = opt[Int](default = Some(10))
  val scapacity: ScallopOption[Int] = opt[Int](default = Some(200))
  val tcapacity: ScallopOption[Int] = opt[Int](default = Some(200))
  val fraction: ScallopOption[Double] = opt[Double](default = Some(0.1))
  val tolerance: ScallopOption[Double] = opt[Double](default = Some(1e-3))
  val debug: ScallopOption[Boolean] = opt[Boolean](short = 'd', default = Some(false), descr = "Enable debug mode")

  verify()

  def epsilon(): Double = epsilon_prime() + tolerance()
  val r: Double = (epsilon_prime() / 2.0) + tolerance()
  val r2: Double = math.pow(epsilon_prime() / 2.0, 2) + tolerance()
  val expansion: Double = epsilon_prime() * 1.5 + tolerance()
}
