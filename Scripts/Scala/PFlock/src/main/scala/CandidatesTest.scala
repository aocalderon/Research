package edu.ucr.dblab.pflock

import scala.collection.JavaConverters._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum}
import edu.ucr.dblab.pflock.Utils.save

object CandidatesTest {
  case class Record(cellId: Int, stage: String, time: Double)

  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .master("local[*]")
      .appName("CandidateTest").getOrCreate()

    import spark.implicits._

    val input = "/home/acald013/Datasets/candidate_query.txt"
    val records = spark.read
      .option("header", false)
      .textFile(input)


    val stats = records.filter(col("value").contains("MAXIMAL")).map { line =>
      val arr = line.split("\\|")
      val cellId = arr(3).toInt
      val stage = arr(12)
      val time = arr(13).toDouble

      Record(cellId, stage, time)
    }.groupBy($"cellId", $"stage").agg(sum($"time")).collect.toList

    save("candidates_query.tsv"){
      stats.map{ row =>
        val cellId = row.getInt(0)
        val stage  = row.getString(1)
        val time   = row.getDouble(2)

        s"$cellId\t$stage\t$time\n"
      }
    }

    /*** Test with textplots library ***/
    /***
    import de.davidm.textplots._
    import org.apache.commons.math3.util.Pair
     
    val plot = new Histogram.HistogramBuilder(
      Pair.create("Time", stats.map(_.getDouble(2)).toArray))
      .setBinNumber(8)
      .plotObject()

    plot.printPlot(true)

    val recs = stats.map{ row =>
      val cellId = row.getInt(0)
      val stage  = row.getString(1)
      val time   = row.getDouble(2)

      Record(cellId, stage, time)
    }

    val data = recs.filter(_.cellId == 1).groupBy(_.stage)
      .mapValues { _.map(_.time).toArray }.toList
      .map{ case(header, serie) =>
        Pair.create(header, serie)
      }.filter(_.getKey == "C differ M")

    data.map(_.getValue.mkString(" ")).foreach{println}

    val p = new Boxplot.BoxplotBuilder(data.asJava).plotObject()
     p.printPlot(true)
    ***/

    spark.close
  }
}
