def isPrime(number: Int): Boolean = {
  number > 1 && (2 until number).filter(i => number % i == 0).size == 0
}

var sum: Double = 0.0
def test1(number: Int): Unit = {
  val start = System.nanoTime
  (0 to number).filter(isPrime).map(x => math.sqrt(x)).foreach(value => sum += value)
  val end = System.nanoTime
  println(s"Test 1 Time: ${(end - start) / 1e9}")
  println(s"Sum = $sum")
}

def test2(number: Int): Unit = {
  val start = System.nanoTime
  (0 to number).par.filter(isPrime).map(x => math.sqrt(x)).foreach(value => sum += value)
  val end = System.nanoTime
  println(s"Test 2 Time: ${(end - start) / 1e9}")
  println(s"Sum = $sum")
}

def test3(number: Int): Unit = {
  val start = System.nanoTime
  val sum = (0 to number).par.filter(isPrime).map(x => math.sqrt(x)).reduce(_ + _)
  val end = System.nanoTime
  println(s"Test 3 Time: ${(end - start) / 1e9}")
  println(s"Sum = $sum")
}

val number: Int = 10000

sum = 0.0
test1(number)
sum = 0.0
test2(number)
test3(number)
