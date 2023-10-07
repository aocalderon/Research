package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.Utils.Disk
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, PrecisionModel}
import streaminer._

object HashesTest {
  def run(oids_prime: String)(implicit G: GeometryFactory): String = {
    val point = G.createPoint(new Coordinate(0, 0))
    val oids = oids_prime.split(",").map(_.toInt).toList
    val disk = Disk(point, oids)

    s"OIDS\t$oids\t${disk.toBinarySignature}"
  }
  def main(args: Array[String]): Unit = {
    implicit val P: HashesParams = new HashesParams(args)
    implicit val G: GeometryFactory = new GeometryFactory(new PrecisionModel(1e3))

    val signature = run(P.oids())

    println(signature)
  }
}

import org.rogach.scallop._

class HashesParams(args: Seq[String]) extends ScallopConf(args) {
  val oids: ScallopOption[String] = opt[String] (default = Some("1,2,3"))
  verify()
}

