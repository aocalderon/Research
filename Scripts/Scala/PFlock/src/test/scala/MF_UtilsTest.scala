package edu.ucr.dblab.pflock

import archery._
import edu.ucr.dblab.pflock.Utils._
import edu.ucr.dblab.pflock.quadtree._
import org.locationtech.jts.geom.{Coordinate, Envelope, GeometryFactory, Point}
import org.slf4j.Logger

import spmf.{KDTree, DoubleArray}

import scala.collection.JavaConverters._
import scala.util.control.Breaks._

import MF_Utils.insertMaximalParallel2

object MF_UtilsTest {
  def main(args: Array[String]): Unit = {
    implicit val G = new GeometryFactory()
    implicit val S = Settings(epsilon_prime = 10)
    implicit val L = org.slf4j.LoggerFactory.getLogger("MyLogger")

    val d1 = Disk(G.createPoint(new Coordinate(0, 0)), List(1,2,3,4))
    val d2 = Disk(G.createPoint(new Coordinate(2, 0)), List(2,3,4))
    val d3 = Disk(G.createPoint(new Coordinate(4, 0)), List(1,2,3,4,5))
    val d4 = Disk(G.createPoint(new Coordinate(6, 0)), List(1,2,3,4))
    val d5 = Disk(G.createPoint(new Coordinate(3, 0)), List(1,2,3,4,5))
    val d6 = Disk(G.createPoint(new Coordinate(5, 0)), List(6,7))

    val cell = Cell(new Envelope(0,0,0,0), 1, "")
    var M = archery.RTree[Disk]()
    M = insertMaximalParallel2(M, d1, cell)
    M = insertMaximalParallel2(M, d2, cell)
    M = insertMaximalParallel2(M, d3, cell)
    M = insertMaximalParallel2(M, d4, cell)
    M = insertMaximalParallel2(M, d5, cell)
    M = insertMaximalParallel2(M, d6, cell)
    
    M.entries.foreach{println}
  }  
}
