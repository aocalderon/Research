
import org.rogach.scallop._

class MFConf(args: Seq[String]) extends ScallopConf(args) {
  val input:       ScallopOption[String]  =  opt[String]   (required = true)
  val offset:      ScallopOption[Int]     =  opt[Int]      (default  = Some(1))
  val epsilon:     ScallopOption[Double]  =  opt[Double]   (default  = Some(10.0))
  val mu:          ScallopOption[Int]     =  opt[Int]      (default  = Some(3))
  val delta:       ScallopOption[Int]     =  opt[Int]      (default  = Some(3))
  val master:      ScallopOption[String]  =  opt[String]   (default  = Some("spark://169.235.27.134:7077"))
  val ppartitions: ScallopOption[Int]     =  opt[Int]      (default  = Some(128))
  val dpartitions: ScallopOption[Int]     =  opt[Int]      (default  = Some(250))
  val cores:       ScallopOption[Int]     =  opt[Int]      (default  = Some(21))
  val grain_x:     ScallopOption[Double]  =  opt[Double]   (default  = Some(10000.0))
  val grain_y:     ScallopOption[Double]  =  opt[Double]   (default  = Some(10000.0))
  val grain_t:     ScallopOption[Double]  =  opt[Double]   (default  = Some(3.0))
  val tag:         ScallopOption[String]  =  opt[String]   (default  = Some(""))
  val debug:       ScallopOption[Boolean] =  opt[Boolean]  (default  = Some(false))

  verify()
}

