import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Column, Row}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.slf4j.{Logger, LoggerFactory}
import scala.annotation.tailrec
import org.andress.Utils._
import org.rogach.scallop._

case class TimeByTime(n: Long, timestamp: String, tag: String, status: String, appId: String,
  executors: Int, cores: Int, timer: Double, stage: String, time: Double, load: Int, instant: Int)

case class TimeByWindow(n: Long, timestamp: String, tag: String, status: String, appId: String,
  executors: Int, cores: Int, timer: Double, stage: String, time: Double, load: Int, instant: Int)

case class Task(n: Long,
  timestamp: String, tag: String, taskId: Int, index: Int, executor: String, duration: Int,
  recordsRead: Long, bytesRead: Long,
  recordsWritten: Long, bytesWritten: Long,
  shuffleReadTime: Long, shuffleWriteTime: Long,
  stageId: Int, jobId: Int, appId: String
)

object TaskInfoAnalizer extends App {
  implicit val logger: Logger = LoggerFactory.getLogger("myLogger")

  def log(msg: String): Unit = logger.info(msg)

  def nrecords(msg: String, n: Long): Unit = log(s"$msg: $n")

  def splitDF(df: DataFrame, column: Column, columns: Array[String], delimiter: String = "\\|"):
      DataFrame = {
    df.withColumn("temp", split(column, delimiter))
      .select{
        (0 until columns.size).map(i => col("temp").getItem(i).as(columns(i))): _*
      }
  }

  @tailrec
  def loopFields(fields: List[StructField], df: DataFrame): DataFrame = {
    fields match {
      case Nil => df
      case head +: tail => {
        val name = head.name
        val dataType = head.dataType
        val df2 = df.withColumn(name + "_1", trim(df(name)).cast(dataType))
          .drop(name)
          .withColumnRenamed(name + "_1", name)
        loopFields(tail, df2)
      }
    }
  }

  def castTimestampColumn(df: DataFrame, columnName: String): DataFrame = {
    val part1 = unix_timestamp(df(columnName), "yyyy-MM-dd HH:mm:ss")
    val part2 = substring(df(columnName), -3, 3).cast("double") / 1000
    df.withColumn("test", (part1 + part2).cast(TimestampType))
      .drop(columnName)
      .withColumnRenamed("test", columnName)
  }

  def setSchema(dataframe: DataFrame, schema: StructType): DataFrame = {
    loopFields(schema.fields.toList, dataframe)
  }

  def splitByEqual(df: DataFrame, column: Column, schema: StructType,
      delimiter: String = "\\|")(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val columns = schema.fieldNames
    
    val temp = splitDF(df, column, columns, delimiter).rdd.map{ row =>
      (0 until columns.size).map{ i =>
        val field = row.getString(i)
        if(field.contains("=")) field.split("=")(1) else field
      }.mkString("|")
    }.toDF("line")
    val dataframe = splitDF(temp, $"line", columns, delimiter)
    setSchema(dataframe, schema)
  }

  def setStartEndByColumn(dataframe: DataFrame, column: Column): DataFrame = {
    val names = dataframe.schema.fieldNames.filter(_ != column.toString)
    val df = dataframe
      .withColumn("start_1", min(column).over(Window.orderBy(column).rowsBetween(-1, -1)) + 1)
      .withColumn("start", when(col("start_1").isNull, 0).otherwise(col("start_1")))
      .withColumnRenamed(column.toString(), "end")
      .drop("start_1")

    val cols =  List(col("start"), col("end")) ++
      df.schema.fieldNames.filter(_ != "start").filter(_ != "end").map(col)

    df.select(cols: _*)
  }

  logger.info("Starting session...")
  implicit val params = new TaskInfoAnalizerConf(args)
  implicit val spark = SparkSession.builder().getOrCreate
  import spark.implicits._
  logger.info("Starting session... Done!")

  val data = timer{"Reading data"}{
    val data = spark.read.text(params.input())
      .withColumn("n", monotonicallyIncreasingId())
      .rdd.map{ row => 
        s"${row.getLong(1)}|${row.getString(0)}"
      }.toDF("line").cache
    nrecords("data", data.count)
    data
  }

  val FE = timer{"Extracting FE"}{
    val FE = data.filter($"line".rlike("\\|FE\\|"))
      .filter($"line".rlike("\\|END  \\|")).cache
    nrecords("FE", FE.count)

    val schema = ScalaReflection.schemaFor[TimeByWindow].dataType.asInstanceOf[StructType]
    val dataframe = splitDF(FE, $"line", schema.fieldNames)
    setStartEndByColumn(setSchema(dataframe, schema), $"n")
  }

  val FF = timer{"Extracting FF"}{
    val FF = data.filter($"line".rlike("\\|FF\\|"))
      .filter($"line".rlike("\\|  END\\|")).cache
    nrecords("FF", FF.count)

    val schema = ScalaReflection.schemaFor[TimeByTime].dataType.asInstanceOf[StructType]
    val dataframe = splitDF(FF, $"line", schema.fieldNames)
    setStartEndByColumn(setSchema(dataframe, schema), $"n")
  }

  val tasks = timer{"Extracting tasks"}{    
    val tasks = data.filter($"line".rlike("TASKINFO")).cache
    nrecords("tasks", tasks.count)

    val schema = ScalaReflection.schemaFor[Task].dataType.asInstanceOf[StructType]
    val dataframe = splitDF(tasks, $"line", schema.fieldNames)
    setSchema(dataframe, schema)
  }

  val joinedFF = timer{"Join tasks and FF tables"}{
    FF.alias("A")
      .join(tasks.alias("B"), $"A.start" <= $"B.n" && $"B.n" <= $"A.end" && $"A.appId" === $"B.appId"
      ).select($"B.timestamp", $"executor", $"duration",
        $"recordsRead", $"bytesRead",
        $"recordsWritten", $"bytesWritten",
        $"shuffleReadTime", $"shuffleWriteTime",
        $"B.taskId", $"B.index", $"B.stageId", $"B.jobId",
        $"A.stage", $"A.time", $"A.instant", $"A.appId")
  }

  val joinedFE = timer{"Join tasks and FE tables"}{
    FE.alias("A")
      .join(tasks.alias("B"), $"A.start" <= $"B.n" && $"B.n" <= $"A.end" && $"A.appId" === $"B.appId"
      ).select($"B.timestamp", $"executor", $"duration",
        $"recordsRead", $"bytesRead",
        $"recordsWritten", $"bytesWritten",
        $"shuffleReadTime", $"shuffleWriteTime",
        $"B.taskId", $"B.index", $"B.stageId", $"B.jobId",
        $"A.stage", $"A.time", $"A.instant", $"A.appId")
  }

  FE.printSchema()
  FE.show(5, truncate = false)

  FF.printSchema()
  FF.show(5, truncate = false)

  tasks.printSchema()
  tasks.show(5, truncate = false)

  joinedFF.orderBy($"duration".desc).show(20, truncate=false)
  nrecords("Number of records after join:", joinedFF.count())

  joinedFE.orderBy($"duration".desc).show(20, truncate=false)
  nrecords("Number of records after join:", joinedFE.count())

  logger.info("Closing session...")
  spark.close()
  logger.info("Closing session... Done!")
}

class TaskInfoAnalizerConf(args: Seq[String]) extends ScallopConf(args) {
  val input = opt[String](default = Some("/home/acald013/Datasets/ValidationLogs/sample2.txt"))
  val appid = opt[String](default = Some("0032"))

  verify()
}
