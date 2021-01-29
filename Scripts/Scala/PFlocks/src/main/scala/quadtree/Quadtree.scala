package edu.ucr.dblab.pflock.quadtree

import com.vividsolutions.jts.geom.Envelope

object Quadtree {  
  def create[T](boundary: Envelope, lineages: List[String]): StandardQuadTree[T] = {
    val quadtree = if(lineages.isEmpty){
      new StandardQuadTree[T](new QuadRectangle(boundary), 0, 1, 0)
    } else {
      val maxLevel = lineages.map(_.size).max
      val quadtree = new StandardQuadTree[T](new QuadRectangle(boundary), 0, 1, maxLevel)
      quadtree.split()
      for(lineage <- lineages.sorted){
        val arr = lineage.map(_.toInt - 48)
        var current = quadtree
        for(position <- arr.slice(0, arr.size - 1)){
          val regions = current.getRegions()
          current = regions(position)
          if(current.getRegions == null){
            current.split()
          }
        }
      }
      quadtree
    }
    quadtree.assignPartitionLineage()
    quadtree.assignPartitionIds()

    quadtree
  }

  def get[T](quadtree: StandardQuadTree[T], lineage: String): StandardQuadTree[T] = {
    var current = quadtree
    for(position <- lineage.map(_.toInt - 48)){
      val regions = current.getRegions()
      current = regions(position)
    }

    current.assignPartitionLineage()
    current.assignPartitionIds()

    current
  }

  def filter[T](quadtree: StandardQuadTree[T], filter: String): StandardQuadTree[T] = {
    if(filter == "*"){
      quadtree
    } else {
      var current = quadtree
      for(position <- filter.map(_.toInt - 48)){
        val regions = current.getRegions()
        current = regions(position)
      }

      current
    }
  }
}
