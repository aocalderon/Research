class Transaction(elements: List[Int], e: Int = 0) extends Ordered[Transaction] {
  var items: List[Int] = elements.sorted
  var len: Int = items.length
  var offset: Int = e

  def prefix(i: Int): List[Int] = { items.slice(0, i) }

  override def equals(obj: scala.Any): Boolean = this.items.equals(obj.asInstanceOf[Transaction].items)

  def compare(that: Transaction): Int = this.items.mkString(" ").compare(that.items.mkString(" "))

  def toItemset: Itemset = new Itemset(this.items)

  override def toString: String = s"{${items.mkString(" ")}}"

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

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + elements.hashCode
    result = prime * result + (if (elements == null) 0 else elements.hashCode)
    result
  }
}
