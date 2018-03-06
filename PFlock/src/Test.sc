import org.apache.spark.sql.SparkSession

val master = "local[*]" //"spark://169.235.27.134:7077"
val simba = SparkSession.builder().master(master)
    .appName("Benchmark")
    .getOrCreate()
