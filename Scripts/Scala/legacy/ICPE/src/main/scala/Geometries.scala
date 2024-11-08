import com.vividsolutions.jts.geom.{GeometryFactory, Point, Coordinate}

case class Disk(x: Double, y: Double, pids: Set[Int], var subset: Boolean = false) extends Ordered[Disk]{
  def count: Int = pids.size

  override def compare(that: Disk): Int = {
    if (x == that.x) y compare that.y
    else x compare that.x
  }

  def canEqual(a: Any) = a.isInstanceOf[Disk]

  override def equals(that: Any): Boolean =
    that match {
      case that: Disk => {
        that.canEqual(this) && this.x == that.x && this.y == that.y
      }
      case _ => false
    }

  override def toString: String = s"${pids.toList.sorted.mkString(" ")}\t$x\t$y"

  def toWKT: String = s"POINT($x $y)\t${pids.toList.sorted.mkString(" ")}\n"
}

case class ST_Point(tid: Int, x: Double, y: Double, t: Int) extends Ordered[ST_Point]{
  val geofactory: GeometryFactory = new GeometryFactory()
  
  def distance(other: ST_Point): Double = {
    math.sqrt(math.pow(this.x - other.x, 2) + math.pow(this.y - other.y, 2))
  }

  def getJTSPoint: Point = {
    val point = geofactory.createPoint(new Coordinate(this.x, this.y))
    point.setUserData(s"${this.tid}\t${this.t}")
    point
  }

  override def compare(that: ST_Point): Int = {
    if (x == that.x) y compare that.y
    else x compare that.x
  }

  def canEqual(a: Any) = a.isInstanceOf[ST_Point]

  override def equals(that: Any): Boolean =
    that match {
      case that: ST_Point => {
        that.canEqual(this) && this.x == that.x && this.y == that.y
      }
      case _ => false
    }

  override def toString: String = s"$tid\t$x\t$y\t$t\n"

  def toWKT: String = s"POINT($x $y)\t$tid\t$t\n"
}
