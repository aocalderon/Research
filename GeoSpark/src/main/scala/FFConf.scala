
import org.rogach.scallop._

class FFConf(args: Seq[String]) extends ScallopConf(args) {
  val input:        ScallopOption[String]  =  opt[String]   (required = true)
  val offset:       ScallopOption[Int]     =  opt[Int]      (default  = Some(1))
  val distance:     ScallopOption[Double]  =  opt[Double]   (default  = Some(100.0))
  val epsilon:      ScallopOption[Double]  =  opt[Double]   (default  = Some(10.0))
  val mu:           ScallopOption[Int]     =  opt[Int]      (default  = Some(3))
  val delta:        ScallopOption[Int]     =  opt[Int]      (default  = Some(3))
  val master:       ScallopOption[String]  =  opt[String]   (default  = Some("169.235.27.138")) //spark://169.235.27.134:7077
  val port:         ScallopOption[String]  =  opt[String]   (default  = Some("7077"))
  val portui:       ScallopOption[String]  =  opt[String]   (default  = Some("4040"))
  val sespg:        ScallopOption[String]  =  opt[String]   (default  = Some("epsg:3068"))
  val tespg:        ScallopOption[String]  =  opt[String]   (default  = Some("epsg:3068"))
  val spatial:      ScallopOption[String]  =  opt[String]   (default  = Some("EQUALGRID"))
  val output:       ScallopOption[String]  =  opt[String]   (default  = Some("/tmp/"))
  val ffpartitions: ScallopOption[Int]     =  opt[Int]      (default  = Some(625))
  val mfpartitions: ScallopOption[Int]     =  opt[Int]      (default  = Some(625))
  val dpartitions:  ScallopOption[Int]     =  opt[Int]      (default  = Some(2))
  val dcustomx:     ScallopOption[Int]     =  opt[Int]      (default  = Some(30))
  val dcustomy:     ScallopOption[Int]     =  opt[Int]      (default  = Some(30))
  val ffcustomx:    ScallopOption[Double]  =  opt[Double]   (default  = Some(10.0))
  val ffcustomy:    ScallopOption[Double]  =  opt[Double]   (default  = Some(10.0))
  val mfcustomx:    ScallopOption[Double]  =  opt[Double]   (default  = Some(10.0))
  val mfcustomy:    ScallopOption[Double]  =  opt[Double]   (default  = Some(10.0))
  val cores:        ScallopOption[Int]     =  opt[Int]      (default  = Some(4))
  val executors:    ScallopOption[Int]     =  opt[Int]      (default  = Some(3))
  val tag:          ScallopOption[String]  =  opt[String]   (default  = Some(""))
  val timestamp:    ScallopOption[Int]     =  opt[Int]      (default  = Some(-1))
  val fftimestamp:  ScallopOption[Int]     =  opt[Int]      (default  = Some(10))
  val ffdebug:      ScallopOption[Boolean] =  opt[Boolean]  (default  = Some(false))
  val mfdebug:      ScallopOption[Boolean] =  opt[Boolean]  (default  = Some(false))
  val mfprint:      ScallopOption[Boolean] =  opt[Boolean]  (default  = Some(false))

  verify()
}

