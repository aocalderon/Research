class Itemset(i: List[Int]) {
  val items: List[Int] = i.sorted
  var len: Int = items.size
  var count: Int = 0
  var denotation: Set[Transaction] = Set.empty[Transaction]
  var closure: List[Int] = List.empty[Int]

  def prefix(i: Int): List[Int] = { items.filter(_ < i) }

  def tail(): Int = { if(items.isEmpty) 0 else items(len - 1) }

  def U(e: Int): Itemset = { new Itemset(this.items :+ e) }

  def nonEmpty(): Boolean = { items.nonEmpty }

  override def toString: String = s"${items.mkString(" ")}"

  def denotationMinusClosure(): List[Transaction] = {
    denotation.map(_.items.filterNot(closure.toSet)).map(d => new Transaction(d)).toList
  }

  def setDenotation(d: Set[Transaction]): Unit = {
    this.denotation = d
    this.closure = setClosure()
  }

  private def setClosure(): List[Int] = this.denotation.map(_.items).reduce((a, b) => a.intersect(b))

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
