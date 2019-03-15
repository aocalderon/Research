import com.vividsolutions.jts.geom.Envelope
import scala.collection.mutable.ListBuffer

class CustomGrid(boundary: Envelope){
  def getGridsBySize(numX: Int, numY: Int): List[Envelope] = {
    var grids: ListBuffer[Envelope] = new ListBuffer()
    val intervalX = (boundary.getMaxX - boundary.getMinX) / numX
    val intervalY = (boundary.getMaxY - boundary.getMinY) / numY

    for(i <- 0 to numX - 1){
      for(j <- 0 to numY - 1){
        val grid = new Envelope(
          boundary.getMinX() + intervalX * i,
          boundary.getMinX() + intervalX * (i + 1),
          boundary.getMinY() + intervalY * j,
          boundary.getMinY() + intervalY * (j + 1)
        )
        grids += grid
      }
    }
    grids.toList
  }

  def getGridsByInterval(intervalX: Double, intervalY: Double): List[Envelope] = {
    var grids: ListBuffer[Envelope] = new ListBuffer()
    val numX = math.floor((boundary.getMaxX - boundary.getMinX) / intervalX).toInt
    val numY = math.floor((boundary.getMaxY - boundary.getMinY) / intervalY).toInt

    for(i <- 0 to numX){
      for(j <- 0 to numY){
        val grid = new Envelope(
          boundary.getMinX() + intervalX * i,
          boundary.getMinX() + intervalX * (i + 1),
          boundary.getMinY() + intervalY * j,
          boundary.getMinY() + intervalY * (j + 1)
        )
        grids += grid
      }
    }
    grids.toList
  }
}
