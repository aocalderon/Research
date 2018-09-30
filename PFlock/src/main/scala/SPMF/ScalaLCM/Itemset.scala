package SPMF.ScalaLCM

import util.control.Breaks._

class Itemset(i: List[Int]) {
  val items: List[Int] = i.sorted
  var clo_tail: Int = -1
  var len: Int = items.size
  var count: Int = 0
  var denotation: Set[Transaction] = Set.empty[Transaction]
  var closure: Itemset = _

  def prefix(i: Int): Itemset = new Itemset(items.filter(_ <= i))

  def U(e: Int): Itemset = { new Itemset(this.items :+ e) }

  def nonEmpty(): Boolean = { items.nonEmpty }

  def isEmpty: Boolean = { items.isEmpty }

  override def toString: String = s"${items.mkString(" ")}"

  def tail(): Int = {
    if(this.items.isEmpty) return 0
    val smeti = this.items.reverse
    var i = smeti.head
    for(j <- smeti){
      val P_j = this.prefix(j)
      P_j.setDenotation(this.denotation)
      if(!P_j.closure.items.equals(this.items)){
        break
      }
      i = j
    }

    i
  }

  def setDenotation(d: Set[Transaction]): Unit = {
    this.denotation = d
    this.closure = new Itemset(this.denotation.map(_.items).reduce((a, b) => a.intersect(b)))
  }

  def getClosure: Itemset = {
    val newP = this.closure
    newP.setDenotation(this.denotation)
    newP.count = this.count

    newP
  }

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + items.hashCode
    result = prime * result + (if (items == null) 0 else items.hashCode)
    result
  }

  override def equals(obj: scala.Any): Boolean = this.items.equals(obj.asInstanceOf[Itemset].items)

  def contains(item: Integer): Int = {
    var low = 0
    var high = len - 1
    while ( {
      high >= low
    }) {
      val middle = (low + high) >>> 1 // divide by 2
      if (items(middle) == item) return middle
      if (items(middle) < item) low = middle + 1
      if (items(middle) > item) high = middle - 1
    }
    -1
  }
}
