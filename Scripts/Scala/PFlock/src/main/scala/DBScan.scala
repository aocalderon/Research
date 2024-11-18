package edu.ucr.dblab.pflock

import edu.ucr.dblab.pflock.Utils._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom._
import org.locationtech.jts.io.WKTReader
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

import edu.ucr.dblab.pflock.spmf.{DoubleArray, AlgoDBSCAN => SpmfDBScan}
import edu.ucr.dblab.pflock.sedona.quadtree.Quadtree

object DBScan {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def main(args: Array[String]): Unit = {
    implicit val params: BFEParams = new BFEParams(args)

    implicit val spark = SparkSession.builder()
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.driver.memory","35g")
      .config("deploy.moce","client")
      .config("spark.executor.memory","20g")
      .config("spark.memory.offHeap.enabled",true)
      .config("spark.memory.offHeap.size","16g")
      .config("spark.driver.maxResultSize","4g")
      .config("spark.kryoserializer.buffer.max","256m")
      .master(params.master())
      .appName("DBScan").getOrCreate()
    import spark.implicits._

    implicit val S = Settings(
      dataset = params.dataset(),
      epsilon_prime = params.epsilon(),
      mu = params.mu(),
      method = params.method(),
      capacity = params.capacity(),
      fraction = params.fraction(),
      tolerance = params.tolerance(),
      tag = params.tag(),
      debug = params.debug(),
      output = params.output(),
      appId = spark.sparkContext.applicationId
    )

    implicit val G = new GeometryFactory(new PrecisionModel(S.scale))

    printParams(args)
    log(s"START|")

    /*******************************************************************************/
    // Code here...
    log("Reading data...")
    val trajs = spark.read
      .option("header", value = false)
      .option("delimiter", "\t")
      .csv(S.dataset)
      .rdd
      .mapPartitions { rows =>
        rows.map { row =>
          val oid = row.getString(0).toInt
          val lon = row.getString(1).toDouble
          val lat = row.getString(2).toDouble
          val tid = row.getString(3).toInt

          val point = G.createPoint(new Coordinate(lon, lat))
          point.setUserData(Data(oid, tid))

          (tid, point)
        }
      }.cache
    val points = trajs.filter(_._1 == 0).map(_._2).cache()
    val nPoints = points.count()
    log(s"$nPoints points read.")


    log("Partitioning...")
    val (cells, tree, rectangle) = Quadtree.build(points, capacity = S.capacity, fraction = S.fraction)
    val pointsRDD = points.flatMap{ point =>
      tree.query(point.getEnvelopeInternal).asScala.toList
        .map{ x =>
          val id = x.asInstanceOf[Int]
          (id, point)
        }
    }
      .partitionBy(MF_Utils.SimplePartitioner(cells.size))
      .map(_._2).cache
    val nPointsRDD = pointsRDD.count()
    log(s"${cells.size} number of cells.")

    log("Running DBScan...")
    case class Cluster(points: List[Point], id: Int, riskZone: Boolean){
      import org.locationtech.jts.algorithm.ConvexHull

      def getConvexHull(implicit G: GeometryFactory): Geometry = {
        if(points.size == 1){
          val envelope = points.head.getEnvelopeInternal
          envelope.expandBy(0.50)
          G.toGeometry(envelope)
        } else { 
          val geom = new ConvexHull(points.map(_.getCoordinate).toArray, G)
          geom.getConvexHull.asInstanceOf[Geometry]
        }
      }

      def getRiskyPoints: List[Point] = points.filter(_.getUserData.asInstanceOf[Data].riskZone)
    }

    val clustersRDD = pointsRDD.mapPartitionsWithIndex{ (index, points_prime) =>
      val cell = new Envelope(cells(index))
      cell.expandBy(S.epsilon * -1.0)

      // Create a list of points from the iterator for reuse...
      val points = points_prime.toList

      // Prepare the points to feed the algorithm...
      val dataSpmf = points.map{ point =>
        new DoubleArray(point)
      }.toList.asJava

      // Run SPMF DBScan algorithm...
      val algo = new SpmfDBScan()

      // Convert the output to a list of Cluster class...
      val clusters = algo.run(dataSpmf, S.mu, S.epsilon).asScala.zipWithIndex.toList
        .map{ case(cluster, id) =>
          cluster.getVectors.asScala.map(_.point)
          val points = cluster.getVectors.asScala.map{ vector =>
            val point = vector.point
            val data = point.getUserData.asInstanceOf[Data]
            val risk_prime = !cell.contains(point.getCoordinate)
            val data_prime = Data(data.id, data.t, risk_prime)
            point.setUserData(data_prime)

            point
          }.toList

          // Check if the cluster touches the risky area...
          val riskZone = points.exists( p => p.getUserData.asInstanceOf[Data].riskZone )

          // Return the cluster...
          Cluster(points, id, riskZone)
        }

      // We have to collect outliers because they could span in contiguous partitions...
      val outliers = points.filter( p => !cell.contains(p.getCoordinate) ).map{ p =>
        Cluster(List(p), -1, true)
      }

      // Return cluster and outliers
      (clusters ++ outliers).toIterator 
    }
    val nClustersRDD = clustersRDD.count()
    val readyClusters = clustersRDD.filter(!_.riskZone)
    val riskyClusters = clustersRDD.filter(_.riskZone) // It includes clusters and outliers...

    log(s"$nClustersRDD number of clusters.")
    log(s"${readyClusters.count()} number of ready clusters.")
    log(s"${riskyClusters.count()} number of risky clusters.")
   
    save("/tmp/edgesClusters.wkt"){
      clustersRDD.map{ cluster =>
        val wkt = cluster.getConvexHull.toText
        val id = cluster.id
        val risk = cluster.riskZone
        
        s"$wkt\t$id\t$risk\n"
      }.collect()
    }

    save("/tmp/edgesCells.wkt"){
      cells.map{ case(id, envelope) =>
        val wkt = G.toGeometry(envelope).toText

        s"$wkt\t$id\n"
      }.toList
    }

    save("/tmp/edgesZones.wkt"){
      cells.map{ case(id, envelope) =>
        val zone = new Envelope(envelope)
        zone.expandBy(S.epsilon * -1.0)
        val wkt = G.toGeometry(zone).toText

        s"$wkt\t$id\n"
      }.toList
    }

    save("/tmp/edgesPoints.wkt"){
      points.map{ point =>
        val wkt = point.toText
        val dat = point.getUserData.asInstanceOf[Data].toString()

        s"$wkt\t$dat\n"
      }.collect()
    }
    /*******************************************************************************/

    spark.close()

    log(s"END|")
  }
}
