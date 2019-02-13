

import org.rogach.scallop._

class FFConf(args: Seq[String]) extends ScallopConf(args) {
  val input:       ScallopOption[String]  =  opt[String]   (required = true)
  val offset:      ScallopOption[Int]     =  opt[Int]      (default  = Some(1))
  val distance:    ScallopOption[Double]  =  opt[Double]   (default  = Some(100.0))
  val epsilon:     ScallopOption[Double]  =  opt[Double]   (default  = Some(10.0))
  val mu:          ScallopOption[Int]     =  opt[Int]      (default  = Some(3))
  val delta:       ScallopOption[Int]     =  opt[Int]      (default  = Some(3))
  val master:      ScallopOption[String]  =  opt[String]   (default  = Some("spark://169.235.27.134:7077"))
  val sespg:       ScallopOption[String]  =  opt[String]   (default  = Some("epsg:3068"))
  val tespg:       ScallopOption[String]  =  opt[String]   (default  = Some("epsg:3068"))
  val ppartitions: ScallopOption[Int]     =  opt[Int]      (default  = Some(12))
  val dpartitions: ScallopOption[Int]     =  opt[Int]      (default  = Some(25))
  val cores:       ScallopOption[Int]     =  opt[Int]      (default  = Some(21))
  val tag:         ScallopOption[String]  =  opt[String]   (default  = Some(""))
  val debug:       ScallopOption[Boolean] =  opt[Boolean]  (default  = Some(false))

  verify()
}
