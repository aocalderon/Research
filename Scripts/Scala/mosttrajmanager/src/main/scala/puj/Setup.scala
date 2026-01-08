package puj

import scopt.OParser

import org.locationtech.jts.geom._

case class Settings(
    input: String = "",
    output: String = "/tmp/",
    master: String = "local[3]",
    crsin: String = "EPSG:4326",
    crsout: String = "EPSG:3944",
    tolerance: Double = 1e-2,
    debug: Boolean = false
) {
  val geofactory: GeometryFactory = new GeometryFactory(new PrecisionModel(1 / tolerance))
}

object Setup {
  val builder = OParser.builder[Settings]
  val parser = {
    import builder._
    OParser.sequence(
      programName("MoST Trajectory Processor"),
      head("MoST Trajectory Processor", "0.1"),
      opt[String]('i', "input")
        .required()
        .action((x, c) => c.copy(input = x))
        .text("Input path for raw data"),
      opt[String]('o', "output")
        .action((x, c) => c.copy(output = x))
        .text("Output path for processed data"),
      opt[String]('m', "master")
        .action((x, c) => c.copy(master = x))
        .text("Number of Spark master nodes"),
      opt[String]("crsin")
        .action((x, c) => c.copy(crsin = x))
        .text("Input CRS SRID (default: EPSG:4326)"),
      opt[String]("crsout")
        .action((x, c) => c.copy(crsout = x))
        .text("Output CRS SRID (default: EPSG:3944)"),
      opt[Double]("tolerance")
        .action((x, c) => c.copy(tolerance = x))
        .text("Tolerance for geometry operations (default: 1e-2)"),
      opt[Boolean]("debug")
        .action((x, c) => c.copy(debug = x))
        .text("Enable debug mode"),

      // Validate inputs immediately
      checkConfig { c =>
        if (c.input.isEmpty) failure("Input path cannot be empty")
        else success
      }
    )
  }
}
