import org.scalatest.flatspec.AnyFlatSpec
import edu.ucr.dblab.pflock.welzl.sec.{SmallestEnclosingCircle, Circle, Point}

class SmallestEnclosingCircleTest extends AnyFlatSpec {
  val rand = scala.util.Random
  val epsilon = 1e-14

  val n = 1000
    (1 to n).foreach{ i =>
      s"Circle $i from linear " should s"be equal to circle $i from naive" in {
        val points = SmallestEnclosingCircle.makeRandomPoints(rand.nextInt(30) + 1)
        val reference = SmallestEnclosingCircle.naive(points)
        val actual    = SmallestEnclosingCircle.linear(points)

        assert(
          math.abs(reference.r - actual.r) < epsilon &&
            math.abs(reference.c.x - actual.c.x) < epsilon &&
            math.abs(reference.c.y - actual.c.y) < epsilon
        )
      }
    }
}
