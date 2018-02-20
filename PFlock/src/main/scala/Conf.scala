import org.rogach.scallop.{ScallopConf, ScallopOption}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val epsilon:    ScallopOption[Double] = opt[Double] (default = Some(1.0))
  val mu:         ScallopOption[Int]    = opt[Int]    (default = Some(3))
  val entries:    ScallopOption[Int]    = opt[Int]    (default = Some(25))
  val partitions: ScallopOption[Int]    = opt[Int]    (default = Some(32))
  val candidates: ScallopOption[Int]    = opt[Int]    (default = Some(256))
  val cores:      ScallopOption[Int]    = opt[Int]    (default = Some(32))
  val master:     ScallopOption[String] = opt[String] (default = Some("spark://169.235.27.134:7077")) /* spark://169.235.27.134:7077 */
  val home:       ScallopOption[String] = opt[String] (default = Some("RESEARCH_HOME"))
  val path:       ScallopOption[String] = opt[String] (default = Some("Datasets/Buses/"))
  val valpath:    ScallopOption[String] = opt[String] (default = Some("Validation/"))
  val dataset:    ScallopOption[String] = opt[String] (default = Some("buses0-1"))
  val extension:  ScallopOption[String] = opt[String] (default = Some("tsv"))
  val separator:  ScallopOption[String] = opt[String] (default = Some("\t"))
  val method:     ScallopOption[String] = opt[String] (default = Some("fpmax"))
  val debug:      ScallopOption[Boolean] = opt[Boolean] (default = Some(false))
  // FlockFinder parameters
  val delta:	    ScallopOption[Int]    = opt[Int]    (default = Some(2))
  val tstart:     ScallopOption[Int]    = opt[Int]    (default = Some(0))
  val tend:       ScallopOption[Int]    = opt[Int]    (default = Some(5))
  val cartesian:  ScallopOption[Int]    = opt[Int]    (default = Some(2))
  val speed:      ScallopOption[Double] = opt[Double] (default = Some(2.0))
  val time:       ScallopOption[Double] = opt[Double] (default = Some(1.0))
  val logs:	      ScallopOption[String] = opt[String] (default = Some("INFO"))
  val output:	    ScallopOption[String] = opt[String] (default = Some("/tmp/"))
  val print:      ScallopOption[Boolean] = opt[Boolean] (default = Some(true))

  verify()
}
