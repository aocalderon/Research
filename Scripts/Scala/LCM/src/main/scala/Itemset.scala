class Itemset(i: List[Int]) {
  val items: List[Int] = i.sorted
  var len: Int = items.size
  var count: Int = 0

  def prefix(i: Int): List[Int] = { items.slice(0, i) }

  def tail(): Int = { if(items.isEmpty) 0 else items(len - 1) }

  def U(e: Int): Itemset = { new Itemset(this.items :+ e) }

  override def toString: String = s"${items.mkString(" ")}"

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
