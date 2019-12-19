import org.apache.spark.sql.SparkSession
import org.apache.spark.TaskContext

object Tester{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Tester")
      .getOrCreate()
    import spark.implicits._

    spark.sparkContext.parallelize(0 until 1000, 10).mapPartitionsWithIndex{ case (index, partition) =>
      val size = partition.size
      val ctx = TaskContext.get
      val input = ctx.taskMetrics().inputMetrics.recordsRead
      val executionTime = ctx.taskMetrics().executorRunTime
      val stageId = ctx.stageId
      val partId = ctx.partitionId
      val hostname = java.net.InetAddress.getLocalHost().getHostName()
      Iterator(s"Index: $index, Size: $size, Stage: $stageId, Partition: $partId, Time: $executionTime, Host: $hostname")
    }.collect.foreach(println)
  }
}
