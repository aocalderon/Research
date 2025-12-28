package puj.partitioning

import org.locationtech.jts.geom.{Envelope, GeometryFactory}    
import puj.Utils.Disk

/**
  * A spatial cell defined by its envelope and associated metadata.
  *
  * @param id
  * @param envelope
  * @param lineage
  * @param G
  */
case class Cell (id: Int, envelope: Envelope, lineage: String = "")(implicit G: GeometryFactory) {
    def wkt: String = G.toGeometry(envelope).toText

    override def toString(): String = s"$wkt\t$id\t$lineage"

    def toText: String = s"${toString()}\n"

    def contains(disk: Disk): Boolean = envelope.contains(disk.center.getCoordinate())
}

object Cell {
  def apply(id: Int, envelope: Envelope, lineage: String = "")(implicit G: GeometryFactory): Cell =
    new Cell(id, envelope, lineage)
}
