package edu.ucr.dblab.pflock

import org.slf4j.{Logger, LoggerFactory}

import org.locationtech.jts.geom._
import org.locationtech.jts.index.strtree.STRtree

import scala.collection.JavaConverters._
import edu.ucr.dblab.pflock.Utils._

import streaminer.SpookyHash

import PSI.logger

object PSI_Utils {
  def main(args: Array[String]): Unit = {
    implicit val params = new BFEParams(args)

    implicit var settings = Settings(
      dataset = params.dataset(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = params.method(),
      capacity = params.capacity(),
      tolerance = params.tolerance(),
      tag = params.tag(),
      debug = params.debug(),
      appId = System.nanoTime().toString()
    )
    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))

    log(s"START")

    val spooky = SpookyHash.getInstance.hash("1235".getBytes)

    log(s"END")
  }
}
