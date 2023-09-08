package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{GeometryFactory, PrecisionModel}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point}

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.io.Source

import sys.process._

import edu.ucr.dblab.pflock.Utils._

object Checker {

  def checkMaximalDisks(ours: List[Disk], theirs: List[Disk], ours_label: String = "ours",
    theirs_label: String = "theirs", points: List[STPoint])
    (implicit G: GeometryFactory, S: Settings, L: Logger): Unit = {

    val ours_pids = ours.map(_.pidsText)
    val ours_pids_file = s"/tmp/${ours_label}_pids.txt"
    save(ours_pids_file){ours_pids.sorted.map(_ + "\n")}

    val theirs_pids = theirs.map(_.pidsText)
    val theirs_pids_file = s"/tmp/${theirs_label}_pids.txt"
    save(theirs_pids_file){theirs_pids.sorted.map(_ + "\n")}

    val diff_output = s"diff -s $ours_pids_file $theirs_pids_file".lineStream_!

    println(diff_output.head)

    val identical = s"Files $ours_pids_file and $theirs_pids_file are identical"
    if(diff_output.head == identical){
      log(s"Maximals|OK!!")
    } else {
      log(s"Maximals|Error!!")
      val (ours_diffs_prime, theirs_diffs_prime) = diff_output
        .filter(l => l.startsWith("<") || l.startsWith(">"))
        .partition(_.startsWith("<"))

      println(s"< $ours_label")
      val ours_diffs = ours_diffs_prime.map(_.substring(2)).toList
      if( !ours_diffs.isEmpty ){
        ours_diffs.foreach{println}
        save(s"/tmp/edges${ours_label}.wkt"){
          for{
            disk <- ours
            key  <- ours_diffs if(disk.pidsText == key)
              } yield {
            disk.getCircleWTK + "\n"
          }
        }
      }

      println(s"> $theirs_label")
      val theirs_diffs = theirs_diffs_prime.map(_.substring(2)).toList
      if( !theirs.isEmpty ){
        theirs_diffs.foreach{println}
        save(s"/tmp/edges${theirs_label}.wkt"){
          for{
            disk <- theirs
            key  <- theirs_diffs if(disk.pidsText == key)
              } yield {
            disk.getCircleWTK + "\n"
          }
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    implicit val logger: Logger = LoggerFactory.getLogger("myLogger")
    implicit val params = new BFEParams(args)

    implicit var S = Settings(
      dataset = params.dataset(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = params.method(),
      capacity = params.capacity(),
      tolerance = params.tolerance(),
      tag = params.tag(),
      debug = params.debug(),
      tester = params.tester(),
      appId = System.nanoTime().toString()
    )
    implicit val geofactory = new GeometryFactory(new PrecisionModel(S.scale))

    val points = readPoints(S.dataset)
    log(s"START")

    log(s"BFE")
    S = S.copy(method="BFE")
    val (maximalsBFE, stats1) = BFE.run(points)
    stats1.print()
    S = S.copy(method="PSI")
    val (maximalsPSI, stats2) = PSI.run(points)
    stats2.printPSI()

    if(S.tester){
      println("Testing...")
      Checker.checkMaximalDisks(maximalsPSI, maximalsBFE, "PSI", "BFE", points)
    }

    log(s"END")
  }  

}
