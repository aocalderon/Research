object Tester{
  case class Person(name: String, children: Person*)

  def main(args: Array[String]): Unit = {
    val x = List(1,2,3,4,5)

    println{
      x.foldLeft(0)(_ + _)
    }

    val bob = Person("Bob")
    val mary = Person("Mary")
    val july = Person("July", bob, mary)
    val peter = Person("Peter", july)

    val people = Vector(bob, mary, july, peter)

    val children = for{
      p <- people if !p.children.isEmpty
    } yield p.name.toUpperCase()

    children.foreach { println }
  }
}
