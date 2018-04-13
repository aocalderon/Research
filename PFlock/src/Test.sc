val Xs = List(1.0, 2.0, 3.0, 4.0, 5.0)
val Ys = List(1.0, 2.0, 3.0, 4.0, 5.0)

val p = Xs.zip(Ys)
val d = getDistanceMatrix(p)
printDistanceMatrix(d)

def printDistanceMatrix(m: Array[Array[Double]]): Unit ={
  println(m.map(_.map("%.2f".format(_)).mkString("\t")).mkString("\n"))
}

def getDistanceMatrix(p: List[(Double, Double)]): Array[Array[Double]] ={
  val n = p.length
  val m = Array.ofDim[Double](n,n)
  for(i <- Range(0, n - 1)){
    for(j <- Range(i + 1, n)) {
      m(i)(j) = dist(p(i), p(j))
    }
  }

  m
}

import Math._
def dist(p1: Tuple2[Double, Double], p2: Tuple2[Double, Double]): Double ={
  sqrt(pow(p1._1 - p2._1, 2) + pow(p1._2 - p2._2, 2))
}

