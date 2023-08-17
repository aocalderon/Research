package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{GeometryFactory, PrecisionModel}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point}

import org.slf4j.Logger

import scala.collection.JavaConverters._
import scala.io.Source

import sys.process._

import Utils._

object Checker {

  def checkMaximalDisks(ours: List[Disk], theirs: List[Disk], points: List[STPoint])
    (implicit G: GeometryFactory, S: Settings, L: Logger): Unit = {

    val ours_pids = ours.map(_.pidsText)
    val ours_pids_file = "/tmp/ours_pids.txt"
    save(ours_pids_file){ours_pids.sorted.map(_ + "\n")}

    val theirs_pids = theirs.map(_.pidsText)
    val theirs_pids_file = "/tmp/theirs_pids.txt"
    save(theirs_pids_file){theirs_pids.sorted.map(_ + "\n")}

    val diff_output = s"diff -s $ours_pids_file $theirs_pids_file".lineStream_!

    if(diff_output == "files are identical"){
      log(s"Maximals|OK!!")
    } else {
      log(s"Maximals|Error!!")
      val (ours_diffs, theirs_diffs) = diff_output.filter(l => l.startsWith("<") || l.startsWith(">"))
        .partition(_.startsWith("<"))

      println("<")
      val diff1 = ours_diffs.map(_.substring(2)).toList
      diff1.foreach{println}

      println(">")
      val diff2 = theirs_diffs.map(_.substring(2)).toList
      diff2.foreach{println}
    }
  }

}
