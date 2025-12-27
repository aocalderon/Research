package puj

import edu.ucr.dblab.pflock.sedona.quadtree.{
  QuadRectangle,
  StandardQuadTree,
  Quadtree
}

import org.apache.spark.{Partitioner, TaskContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.scala.Logging

import org.locationtech.jts.geom._
import org.locationtech.jts.index.strtree.GeometryItemDistance

import scala.collection.JavaConverters._
import scala.util.Random

import puj.Setup._
import puj.Utils._

object P3D extends Logging {
  
  def main(args: Array[String]): Unit = {
    logger.info("Starting P3D application")
    implicit var S: Settings = Setup.getSettings(args) // Initializing settings...
    implicit val G = new GeometryFactory(
      new PrecisionModel(1.0 / S.tolerance)
    )

    implicit val spark: SparkSession = SparkSession
      .builder()
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .master(S.master)
      .appName("P3D")
      .getOrCreate()
    import spark.implicits._
    logger.info("SparkSession created")

    val pointsRDD = spark.read
      .option("header", value = false)
      .option("delimiter", "\t")
      .csv(S.dataset)
      .rdd
      .map { row =>
        val oid = row.getString(0).toInt
        val lon = row.getString(1).toDouble
        val lat = row.getString(2).toDouble
        val tid = row.getString(3).toDouble.toInt

        val point = G.createPoint(new Coordinate(lon, lat))
        point.setUserData(Data(oid, tid))
        point
      }
      .cache()
    val n = pointsRDD.count()
    logger.info(s"Total points loaded: $n")

    val (quadtree, cells, rtree, universe) =
      Quadtree.build(pointsRDD, new Envelope(), capacity = S.scapacity, fraction = S.fraction)
    logger.info(s"Quadtree built with ${cells.size} cells")

    debug{
      saveAsTSV(
        "/tmp/Q.wkt",
        cells.values.map { cell =>
          val wkt = G.toGeometry(cell.envelope).toText()
          val cid = cell.id
          s"$wkt\t$cid\n"
        }.toList
      )
      logger.info("Quadtree WKT file saved for debugging")
    }

    val pointsSRDD = pointsRDD
      .mapPartitions { iter =>
        iter.map { point =>
          val cid = rtree
            .query(point.getEnvelopeInternal())
            .asScala
            .head
            .asInstanceOf[Int]
          (cid, point)
        }
      }
      .partitionBy(SimplePartitioner(cells.size))
      .map(_._2)
      .cache()
    val count = pointsSRDD.count()
    logger.info(s"Points repartitioned into SRDD with $count points")
    
    val THist = pointsSRDD
      .mapPartitions { it =>
        val partitionId = TaskContext.getPartitionId()
        it.map { point =>
          point.getUserData().asInstanceOf[Data].tid
        }.toList
          .groupBy(tid => tid)
          .map { case (tid, list) =>
            (tid, list.size)
          }
          .toIterator
      }
      .groupByKey()
      .map { case (tid, counts) =>
        val total = counts.sum
        Bin(tid, total)
      }
    logger.info("Temporal histogram (THist) computed")

    debug{
      saveAsTSV(
        "/tmp/THist.tsv",
        THist
          .collect()
          .sortBy(_.instant)
          .map(bin => s"${bin.toString()}\n")
          .toList
      )
      logger.info("Temporal histogram TSV file saved for debugging")
    }

    implicit val intervals = Interval.groupInstants(THist.collect().sortBy(_.instant).toSeq, capacity = S.tcapacity)
      .map{ group =>
        val begin = group.head.instant
        val end = group.last.instant
        val number_of_times = group.map(_.count).sum
        
        (begin, end, number_of_times)
      }.zipWithIndex
      .map{ case ((begin, end, number_of_times), index) =>
        (index, Interval(index, begin, end, number_of_times))
      }.toMap

    debug{
      saveAsTSV(
        "/tmp/Intervals.tsv",
        intervals.values
          .toList
          .sortBy(_.index)
          .map(interval => interval.toText)
      )
      logger.info("Temporal intervals TSV file saved for debugging")
    }
    logger.info(s"Total temporal intervals created: ${intervals.size}")

    val temporal_bounds = intervals.values.map(_.begin).toArray.sorted

    val pointsSTRDD_prime = pointsSRDD.mapPartitionsWithIndex{ (spatial_index, it) =>
      it.map { point =>
        val data = point.getUserData().asInstanceOf[Data]
        val tid = data.tid

        val temporal_index = Interval.findInterval(temporal_bounds, tid).index
        val ST_Index = BitwisePairing.encode(spatial_index, temporal_index)
        (ST_Index, point)
      }
    }.cache()

    val st_indexes = pointsSTRDD_prime
      .map{ case (st_index, point) => st_index }
      .distinct()
      .collect()
      .zipWithIndex.toMap
    val st_indexes_reverse = st_indexes.map{ case (k, v) => (v, k) }  
    logger.info(s"Total distinct ST_Indexes: ${st_indexes.size}")
    
    val pointsSTRDD = pointsSTRDD_prime.sample(withReplacement=false, fraction=0.01, seed=42)
      .map{ case (st_index, point) => (st_indexes(st_index), point) }
      .partitionBy(SimplePartitioner(st_indexes.size))
      .map(_._2)
      .cache()
    val nPointsSTRDD = pointsSTRDD.count()
    logger.info(s"Points repartitioned into STRDD with $nPointsSTRDD points")

    debug{
      pointsSTRDD.mapPartitions{ points => 
        val partitionId = TaskContext.getPartitionId()
        val st_index = st_indexes_reverse(partitionId)
        val (s_index, t_index) = BitwisePairing.decode(st_index)
        val wkts = points.map{ point => 
          val i = point.getUserData().asInstanceOf[Data].oid
          val x = point.getX 
          val y = point.getY
          val t = point.getUserData().asInstanceOf[Data].tid
          val wkt = point.toText()
          s"$i\t$x\t$y\t$t\t$s_index\t$t_index\t$st_index\t$wkt\n"
        }.mkString("")
        Iterator( (s_index, wkts) )
      }.collect().groupBy(_._1).foreach{ case (s_index, wkts) =>
        saveAsTSV(
          s"/tmp/STRDD_$s_index.wkt",
          wkts.map(_._2).toList
        )
      }
      logger.info("STRDD WKT file saved for debugging")

      pointsSTRDD.mapPartitions{ points => 
        val partitionId = TaskContext.getPartitionId()
        val st_index = st_indexes_reverse(partitionId)
        val (s_index, t_index) = BitwisePairing.decode(st_index)
        val cell = cells(s_index)
        val interval = intervals(t_index)
        
        val wkt =s"${cell.wkt}\t$st_index\t$s_index\t$t_index\t${interval.begin}\t${interval.duration}\n"
        Iterator( (s_index, wkt) )
      }.collect().groupBy(_._1).foreach{ case (s_index, wkts) =>
        saveAsTSV(
          s"/tmp/Boxes_$s_index.wkt",
          wkts.map(_._2).toList
        )
      }
      logger.info("Boxes WKT file saved for debugging")
    }

    logger.info("SparkSession closed")
  }
}


