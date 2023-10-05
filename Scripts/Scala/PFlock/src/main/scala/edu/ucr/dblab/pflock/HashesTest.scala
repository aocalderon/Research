package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.Utils.Disk
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, PrecisionModel}
import streaminer._

object HashesTest {
  def main(args: Array[String]): Unit = {
    implicit val P: HashesParams = new HashesParams(args)
    implicit val G: GeometryFactory = new GeometryFactory(new PrecisionModel(1e3))
    val point = G.createPoint(new Coordinate(0,0))
    val oids  = P.oids().split(",").map(_.toInt).toList
    val disk = Disk(point, oids)

    println(s"OIDS\t${P.oids()}\t${disk.toBinarySignature}")
  }
}

import org.rogach.scallop._

class HashesParams(args: Seq[String]) extends ScallopConf(args) {
  val oids: ScallopOption[String] = opt[String] (default = Some("1,2,3"))
  verify()
}

