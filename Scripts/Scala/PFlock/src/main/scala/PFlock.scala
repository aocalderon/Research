package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.Utils._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, PrecisionModel}
import org.slf4j.{Logger, LoggerFactory}

object PFlock {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params = new BFEParams(args)

    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .master(params.master())
      .appName("PFlock").getOrCreate()
    import spark.implicits._

    implicit val S = Settings(
      dataset = params.dataset(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      delta = params.delta(),
      capacity = params.capacity(),
      fraction = params.fraction(),
      tolerance = params.tolerance(),
      tag = params.tag(),
      debug = params.debug(),
      print = params.print(),
      output = params.output(),
      iindex = params.iindex(),
      method = params.method(),
      appId = spark.sparkContext.applicationId
    )

    implicit val G = new GeometryFactory(new PrecisionModel(S.scale))

    printParams(args)
    log(s"START|")

    /*******************************************************************************/
    // Code here...
    var t0 = clocktime
    val trajs = spark.read
      .option("header", false)
      .option("delimiter", "\t")
      .csv(S.dataset)
      .rdd
      .mapPartitions{ rows =>
        rows.map{ row =>
          val oid = row.getString(0).toInt
          val lon = row.getString(1).toDouble
          val lat = row.getString(2).toDouble
          val tid = row.getString(3).toInt

          val point = G.createPoint(new Coordinate(lon, lat))
          point.setUserData(Data(oid, tid))

          (tid, STPoint(point))
        }
      }.groupByKey().sortByKey()

    debug{
      trajs.foreach(println)
    }

    val trajs_list = trajs.collect().toList
    val flocks = PF_Utils.join(trajs_list, List.empty[Disk], List.empty[Disk])
    val t1 = clocktime
    val time = (t1 - t0) / 1e9

    logt(s"$time")
    log(s"${flocks.size}")
    save("/home/acald013/tmp/flockss.tsv") {
      flocks.map{ f =>
        val s = f.start
        val e = f.end
        val p = f.pidsText

        s"$s\t$e\t$p\n"
      }.sorted
    }

    /*******************************************************************************/

    spark.close()

    log(s"END|")
  }
}
