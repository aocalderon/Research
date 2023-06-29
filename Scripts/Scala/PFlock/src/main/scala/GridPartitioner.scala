package edu.ucr.dblab.pflock

import org.locationtech.jts.geom.{PrecisionModel, GeometryFactory}
import org.locationtech.jts.geom.{Envelope, Coordinate, Point}

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.Partitioner

import edu.ucr.dblab.pflock.sedona.quadtree._
import edu.ucr.dblab.pflock.Utils._

object GridPartitioner {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params = new BFEParams(args)

    val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .appName("GridPartitioner").getOrCreate()
    import spark.implicits._

    implicit var settings = Settings(
      input = params.input(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = "GridPartitioner",
      capacity = params.capacity(),
      fraction = params.fraction(),
      tolerance = params.tolerance(),
      tag = params.tag(),
      debug = params.debug()
    )

    settings.appId = spark.sparkContext.applicationId
    implicit val geofactory = new GeometryFactory(new PrecisionModel(settings.scale))

    printParams(args)

    val ( (pointsRaw, nRead), tRead) = timer{
      val pointsRaw = spark.read
        .option("delimiter", "\t")
        .option("header", false)
        .textFile(settings.input).rdd
        .map { line =>
          val arr = line.split("\t")
          val i = arr(0).toInt
          val x = arr(1).toDouble
          val y = arr(2).toDouble
          val t = arr(3).toInt
          val point = geofactory.createPoint(new Coordinate(x, y))
          point.setUserData(Data(i, t))
          point
        }.cache
      val nRead = pointsRaw.count
      (pointsRaw, nRead)
    }
    log(s"Read|$nRead")
    logt(s"Read|$tRead")

    val minX = pointsRaw.map(_.getX).min()
    val minY = pointsRaw.map(_.getY).min()

    val (gridRDD, tGrid) = timer{
      val grid = pointsRaw.mapPartitions{ points =>
        val grid = Grid(points.map(p => STPoint(p)).toList)
          .buildGrid1_5(minX: Double, minY: Double)
        // for each non-empty cell...
        grid.keys.map{ key =>
          val (i, j) = decode(key) // position (i, j) for current cell...

          val indices = List( // computing positions (i, j) around current cell...
            (i-1, j+1),(i, j+1),(i+1, j+1),
            (i-1, j)  ,(i, j)  ,(i+1, j),
            (i-1, j-1),(i, j-1),(i+1, j-1)
          ).filter(_._1 >= 0).filter(_._2 >= 0) // just keep positive (i, j)...

          val Ps = indices.flatMap{ case(i, j) => // getting points around current cell...
            val ij = encode(i, j)
            if(grid.keySet.contains(ij))
              grid(ij).map{ point => (key.toInt, point) }
            else
              List.empty[(Int, STPoint)]
          }
          Ps
        }.filter(_.size >= settings.mu).flatten.toIterator
      }.cache
      grid.count
      grid
    }
    val partitions = gridRDD.map(_._1).distinct.collect.toList.sorted.zipWithIndex.toMap
    log(s"Grid|${partitions.size}")
    logt(s"Grid|$tGrid")

    partitions.keys.toList.sorted.map( pid => s"$pid -> ${partitions(pid)}").take(10).foreach{println}

    val ( (pointsRDD, nShuffle), tShuffle) = timer{
      val pointsRDD = gridRDD.partitionBy(new Partitioner {
        def numPartitions: Int = partitions.size
        def getPartition(key: Any): Int = partitions(key.asInstanceOf[Int])
      }).cache
        .map(_._2).cache
      val nShuffle = pointsRDD.count
      (pointsRDD, nShuffle)
    }
    log(s"Shuffle|$nShuffle")
    logt(s"Shuffle|$tShuffle")

    val ( (maximalsRDD, nRun), tRun) = timer{
      val maximalsRDD = pointsRDD.mapPartitionsWithIndex{ case(cid, it) =>
        //val cell = cells(cid)
        val points = it.toList
        val (maximals, stats) = BFE.run(points)

        maximals.toIterator
      }.cache
      val nRun = maximalsRDD.count
      (maximalsRDD, nRun)
    }
    log(s"Run|$nRun")
    logt(s"Run|$tRun")

    debug{
      save("/tmp/edgesMF.wkt"){
        maximalsRDD.mapPartitionsWithIndex{ case(cid, it) =>
          it.map{ p => s"${p.wkt}\t$cid\n"}
        }.collect
      }
    }
    
    val maximalsMF = maximalsRDD.collect.toList
    spark.close()

    log(s"Done.|END")
  }
}
