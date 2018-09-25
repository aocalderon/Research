import org.rogach.scallop.{ScallopConf, ScallopOption}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val epsilon:    ScallopOption[Double] 	= opt[Double] 	(default = Some(10.0))
  val precision:  ScallopOption[Double] 	= opt[Double] 	(default = Some(0.001))
  val mu:         ScallopOption[Int]    	= opt[Int]    	(default = Some(4))
  val partitions: ScallopOption[Int]    	= opt[Int]    	(default = Some(1024))
  val cores:      ScallopOption[Int]    	= opt[Int]    	(default = Some(32))
  val master:     ScallopOption[String] 	= opt[String] 	(default = Some("spark://169.235.27.134:7077")) /* spark://169.235.27.134:7077 */
  val home:       ScallopOption[String] 	= opt[String] 	(default = Some("RESEARCH_HOME"))
  val path:       ScallopOption[String] 	= opt[String] 	(default = Some("Datasets/Berlin/"))
  val valpath:    ScallopOption[String] 	= opt[String] 	(default = Some("Validation/"))
  val dataset:    ScallopOption[String] 	= opt[String]	(default = Some("berlin0-2"))
  val extension:  ScallopOption[String] 	= opt[String] 	(default = Some("tsv"))
  val separator:  ScallopOption[String] 	= opt[String] 	(default = Some("\t"))
  val method:     ScallopOption[String] 	= opt[String] 	(default = Some("lcm"))
  val debug:      ScallopOption[Boolean]	= opt[Boolean]	(default = Some(false))
  // FlockFinder parameters
  val delta:	  ScallopOption[Int]    	= opt[Int]    	(default = Some(3))
  val decimals:	  ScallopOption[Int]    	= opt[Int]    	(default = Some(3))
  val tstart:     ScallopOption[Int]    	= opt[Int]    	(default = Some(0))
  val tend:       ScallopOption[Int]    	= opt[Int]    	(default = Some(10))
  val cartesian:  ScallopOption[Int]    	= opt[Int]    	(default = Some(2))
  val speed:      ScallopOption[Double] 	= opt[Double] 	(default = Some(1.0))
  val time:       ScallopOption[Double] 	= opt[Double] 	(default = Some(1.0))
  val logs:	  ScallopOption[String] 	= opt[String] 	(default = Some("INFO"))
  val output:	  ScallopOption[String] 	= opt[String] 	(default = Some("/tmp/"))
  val print:      ScallopOption[Boolean] 	= opt[Boolean] 	(default = Some(false))
  val epsilon_max:  	ScallopOption[Double] 	= opt[Double] 	(default = Some(10.0))
  val epsilon_step: 	ScallopOption[Double] 	= opt[Double] 	(default = Some(1.0))
  val mu_max:   	ScallopOption[Int]    	= opt[Int]    	(default = Some(4))
  val mu_step:	  	ScallopOption[Int]    	= opt[Int]    	(default = Some(1))
  val delta_max:   	ScallopOption[Int]    	= opt[Int]	(default = Some(3))
  val delta_step:  	ScallopOption[Int]    	= opt[Int]    	(default = Some(1))
  val expansion_size:   ScallopOption[Int]      = opt[Int]      (default = Some(120))

  verify()
}
