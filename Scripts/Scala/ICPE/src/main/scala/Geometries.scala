import com.vividsolutions.jts.geom.{GeometryFactory, PrecisionModel, Envelope, Coordinate, Point, Polygon}

case class Flock(items: Array[Int], start: Int, end: Int){
  var subset = false

  def getItemset: Set[Int] = items.toSet

  def getItems: List[Int] = items.toList.sorted

  def size: Int = items.size
  
  override def toString(): String = s"${getItems.mkString(" ")}\t$start\t$end"
}

case class TDisk(t: Int, disk: Disk) extends Ordered[TDisk]{
  override def compare(that: TDisk): Int = {
    if (t == that.t) disk compare that.disk
    else t compare that.t
  }

  def canEqual(a: Any) = a.isInstanceOf[TDisk]

  override def equals(that: Any): Boolean =
    that match {
      case that: TDisk => {
        that.canEqual(this) && this.t == that.t && this.disk == that.disk
      }
      case _ => false
    }
  override def toString(): String = s"(${disk.x}, ${disk.y}, $t): ${disk.pids.toList.sorted.mkString(" ")}"
}

case class Disk(x: Double, y: Double, pids: Set[Int], var subset: Boolean = false) extends Ordered[Disk]{
  val model: PrecisionModel = new PrecisionModel(1000)
  val geofactory: GeometryFactory = new GeometryFactory(model)

  def count: Int = pids.size

  def getItemset: Set[Int] = pids

  def getItems: List[Int] = pids.toList.sorted

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

  def toWKT: String = s"POINT($x $y)\t${pids.toList.sorted.mkString(" ")}"

  def toPoint: Point = {
    val coord = new Coordinate(x, y)
    val point = geofactory.createPoint(coord)
    point.setUserData(s"${pids.mkString(" ")}")
    point
  }
}

case class ST_Point(tid: Int, x: Double, y: Double, t: Int) extends Ordered[ST_Point]{
  val model: PrecisionModel = new PrecisionModel(1000)
  val geofactory: GeometryFactory = new GeometryFactory(model)
  
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

import org.apache.spark.rdd.RDD
case class DiskPartitioner(boundary: Envelope, width: Double) {
  private val model: PrecisionModel = new PrecisionModel(1000)
  private val geofactory: GeometryFactory = new GeometryFactory(model)
  private val maxX: Double = boundary.getMaxX
  private val minX: Double = boundary.getMinX
  private val minY: Double = boundary.getMinY
  private val maxY: Double = boundary.getMaxY
  private val columns: Int = math.floor((maxX - minX) / width).toInt + 1
  private val rows: Int = math.floor((maxY - minY) / width).toInt + 1

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }

  def getNumPartitions: Int = rows * columns

  private def getIndices(x: Double, y: Double): (Int, Int) = {
    val i = math.floor((x - minX) / width).toInt
    val j = math.floor((y - minY) / width).toInt
    (i, j)
  }

  private def getKey(x: Double, y: Double): Int = {
    val (i, j) = getIndices(x, y)
    i + j * columns
  }

  def indexByExpansion(tdisk: TDisk, expansion: Double): List[(Int, TDisk)] = {
    val disk = tdisk.disk
    val key = getKey(disk.x, disk.y)
    val partition = List((key, tdisk))

    val x1 = disk.x - expansion
    val x2 = disk.x + expansion
    val y1 = disk.y - expansion
    val y2 = disk.y + expansion

    val is = getIndices(x1, y1)
    val js = getIndices(x2, y2)

    val Skeys = (is._1 to js._1).cross(is._2 to js._2).map{ c =>
      c._1 + c._2 * columns
    }.filter(_ != key).filter(_ >= 0).toList

    val neighborhood = Skeys.map(key => (key, tdisk))

    partition ++ neighborhood        
  }

  def getGrids(): List[(Int, Polygon)] = {    
    val Xs = minX to maxX by width
    val Ys = minY to maxY by width
    Xs.cross(Ys).map{ coord =>
      val x = coord._1
      val y = coord._2
      val p1 = new Coordinate(x, y)
      val p2 = new Coordinate(x + width, y)
      val p3 = new Coordinate(x + width, y + width)
      val p4 = new Coordinate(x, y + width)
      val coords = Array(p1,p2,p3,p4,p1)
      val grid = geofactory.createPolygon(coords)
      val key = getKey(x, y)

      (key, grid)
    }.toList
  }
}

import org.apache.spark.rdd.RDD
case class DiskIndex(disks: RDD[Disk], boundary: Envelope, width: Double, expansion: Double) {
  private val model: PrecisionModel = new PrecisionModel(1000)
  private val geofactory: GeometryFactory = new GeometryFactory(model)
  private val maxX: Double = boundary.getMaxX
  private val minX: Double = boundary.getMinX
  private val minY: Double = boundary.getMinY
  private val maxY: Double = boundary.getMaxY
  private val columns: Int = math.floor((maxX - minX) / width).toInt + 1
  private val rows: Int = math.floor((maxY - minY) / width).toInt + 1

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for { x <- xs; y <- ys } yield (x, y)
  }

  private def getIndices(x: Double, y: Double): (Int, Int) = {
    val i = math.floor((x - minX) / width).toInt
    val j = math.floor((y - minY) / width).toInt
    (i, j)
  }

  private def getKey(x: Double, y: Double): Int = {
    val (i, j) = getIndices(x, y)
    i + j * columns
  }

  def index(): RDD[Disk] = {
    disks.flatMap{ disk =>
      val key = getKey(disk.x, disk.y)

      val partition = List((key, disk))

      val x1 = disk.x - expansion; val x2 = disk.x + expansion;
      val y1 = disk.y - expansion; val y2 = disk.y + expansion;
      val is = getIndices(x1, y1)
      val js = getIndices(x2, y2)
      val Skeys = (is._1 to js._1).cross(is._2 to js._2).map{ c =>
        c._1 + c._2 * columns
      }.filter(_ != key).toList
      val neighborhood = Skeys.map(key => (key, disk))
        
      partition ++ neighborhood
    }.partitionBy(new KeyPartitioner(rows * columns)).map(_._2)
  }

  def getGrids(): List[(Int, Polygon, Polygon)] = {    
    val Xs = minX to maxX by width
    val Ys = minY to maxY by width
    Xs.cross(Ys).map{ coord =>
      val x = coord._1
      val y = coord._2
      val p1 = new Coordinate(x, y)
      val p2 = new Coordinate(x + width, y)
      val p3 = new Coordinate(x + width, y + width)
      val p4 = new Coordinate(x, y + width)
      val coords = Array(p1,p2,p3,p4,p1)
      val grid = geofactory.createPolygon(coords)
      val expansion_grid = grid.getEnvelopeInternal
      expansion_grid.expandBy(expansion)
      val key = getKey(x, y)

      (key, grid, envelope2Polygon(expansion_grid))
    }.toList
  }

  def envelope2Polygon(e: Envelope): Polygon = {
    val minX = e.getMinX()
    val minY = e.getMinY()
    val maxX = e.getMaxX()
    val maxY = e.getMaxY()
    val p1 = new Coordinate(minX, minY)
    val p2 = new Coordinate(minX, maxY)
    val p3 = new Coordinate(maxX, maxY)
    val p4 = new Coordinate(maxX, minY)
    geofactory.createPolygon(Array(p1,p2,p3,p4,p1))
  }  
}

import org.apache.spark.Partitioner
class KeyPartitioner(override val numPartitions: Int) extends Partitioner {
  override def  getPartition(key: Any): Int = {
    key.asInstanceOf[Int]
  }
  override def equals(other: Any): Boolean = {
    other match {
      case obj: KeyPartitioner => obj.numPartitions == numPartitions
      case _ => false
    }
  }
}
