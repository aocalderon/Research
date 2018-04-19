import scala.collection.mutable.ListBuffer
import scala.io.Source

object FlockChecker {
  private var epsilon = 0.0
  private var mu = 0
  private var delta = 0

  def readFile(path: String): List[String] = {
    var flocks = ListBuffer[String]()
    val file = Source.fromFile(path)
    for (flock <- file.getLines) {
      if(flock != "" || flock != "\n"){
        flocks += flock
      }
    }
    file.close()

    flocks.toList
  }

  def compareFiles(path1: String, path2: String): Unit = {
    val method1 = path1.split("/").last.split("_").head
    val method2 = path2.split("/").last.split("_").head
    val flocks1 = readFile(path1)
    val flocks2 = readFile(path2)

    val notFound = flocks1 filterNot (flocks2 contains)
    val fails = notFound.length
    notFound.foreach(println)

    val n = flocks1.size
    val hits = n - fails
    val p = (hits.toFloat / n.toFloat) * 100.0
    println(s"$method1 vs $method2, $hits / $n, %.2f%%, $epsilon, $mu, $delta".format(p))
    if(hits != n){
      new java.io.PrintWriter(s"/tmp/NotFound-${method1}_E${epsilon}_M${mu}_$delta.flocks") {
	      write(notFound.mkString("\n"))
	      close()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val path1 = args(0)
    val path2 = args(1)
    val params = path2.split("/").last.split("\\.").head.split("_")
    epsilon = params(1).substring(1).toInt
    mu = params(2).substring(1).toInt
    delta = params(3).substring(1).toInt

    FlockChecker.compareFiles(path1,path2)
    FlockChecker.compareFiles(path2,path1)
  }

}
