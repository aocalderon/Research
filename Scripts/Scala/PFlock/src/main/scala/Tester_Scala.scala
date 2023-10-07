package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.Utils._
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, LineString, PrecisionModel}
import org.slf4j.{Logger, LoggerFactory}

import sys.process._

import scala.collection.JavaConverters._
import scala.io.Source

import edu.ucr.dblab.pflock.{HashesTest => scala_hashes}

object Tester_Scala {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params = new BFEParams(args)

    implicit var settings = Settings(
      dataset = params.dataset(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = "PFlocks",
      capacity = params.capacity(),
      fraction = params.fraction(),
      tolerance = params.tolerance(),
      tag = params.tag(),
      debug = params.debug(),
      output = params.output()
    )

    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))

    printParams(args)
    log(s"START|")

    val c_command = "/opt/bfe_modified/build/checkHashes -o "

    val buffer = Source.fromFile(params.dataset())
    val data = buffer.getLines().map{ line =>
      val arr = line.split("\t")
      val id = arr(0).toInt
      val oids = arr(1).replace(" ", ",")

      val signature_c = s"$c_command $oids".lineStream_!
      val signature1  = signature_c.toList.last.split("\t").last
      val signature_s = scala_hashes.run(oids)
      val signature2  = signature_s.split("\t").last

      s"$id\t$oids\t$signature1\t$signature2\n"
    }.toList
    buffer.close()

    val f = new java.io.FileWriter(params.output())
    data.foreach{ record =>
      f.write(record)
    }
    f.close

    log(s"END|")
  }
}
