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

case class StageInfo(n: Long, timestamp: String, tag: String, name: String, ntasks: Int,
  duration: Long, stageId: Int, jobId: Int, appId: String
)

case class StageDetails(n: Long, timestamp: String, stageId: Int, details: String)

object StageInfoAnalizer extends App {
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
    df.withColumn(columnName + "_1", (part1 + part2).cast(TimestampType))
      .drop(columnName)
      .withColumnRenamed(columnName + "_1", columnName)
  }

  def setSchema(dataframe: DataFrame, schema: StructType): DataFrame = {
    loopFields(schema.fields.toList, dataframe)
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
  implicit val params = new StageInfoAnalizerConf(args)
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

  val FF = timer{"Extracting FF"}{
    val FF = data.filter($"line".rlike("\\|FF\\|"))
      .filter($"line".rlike("\\|  END\\|")).cache
    nrecords("FF", FF.count)

    val schema = ScalaReflection.schemaFor[TimeByTime].dataType.asInstanceOf[StructType]
    val dataframe = splitDF(FF, $"line", schema.fieldNames)
    setStartEndByColumn(setSchema(dataframe, schema), $"n")
  }

  val stages = timer{"Extracting stages"}{    
    val stages = data.filter($"line".rlike("STAGEINFO")).cache
    nrecords("Stages", stages.count)

    val schema = ScalaReflection.schemaFor[StageInfo].dataType.asInstanceOf[StructType]
    val dataframe = splitDF(stages, $"line", schema.fieldNames)
    setSchema(dataframe, schema)
  }

  val details = timer{"Extracting stages details"}{    
    val details = data.filter($"line".rlike("STAGEDETAILS")).cache
    nrecords("Details", stages.count)

    val schema = ScalaReflection.schemaFor[StageDetails].dataType.asInstanceOf[StructType]
    val dataframe = splitDF(details, $"line", schema.fieldNames)
    setSchema(dataframe, schema)
  }

  val joinedFF = timer{"Join tasks and FF tables"}{
    val stages2 = stages.alias("s").join(details.alias("d"), $"s.stageId" === $"d.stageId")

    stages2
    
    //FF.alias("A")
      //.join(stages2.alias("B"), $"A.start" <= $"B.n" && $"B.n" <= $"A.end" && $"A.appId" === $"B.appId"
      //).select($"B.timestamp", $"B.stageId", $"B.name", $"B.ntasks", $"B.duration", 
      //  $"A.stage", $"A.time", $"A.instant", $"A.appId")
  }

  FF.printSchema()
  FF.show(5, truncate = false)

  stages.printSchema()
  stages.show(5, truncate = false)

  joinedFF.show()

  //joinedFF.orderBy($"duration".desc).show(params.n(), truncate=false)
  //nrecords("Number of records after join:", joinedFF.count())

  logger.info("Closing session...")
  spark.close()
  logger.info("Closing session... Done!")
}

class StageInfoAnalizerConf(args: Seq[String]) extends ScallopConf(args) {
  val input = opt[String](default = Some("/home/acald013/Datasets/ValidationLogs/sample2.txt"))
  val appid = opt[String](default = Some(""))
  val n = opt[Int](default = Some(20))

  verify()
}
