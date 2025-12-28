package puj.partitioning

object BitwisePairing {

  // We use 16 bits for the shift because 'y' (max 2000) fits easily
  // into 16 bits (max 65535), leaving the upper 16 bits for 'x'.
  val SHIFT_SIZE = 16
  val Y_MASK     = 0xffff // Binary mask for the lower 16 bits (1111111111111111)

  /** Packs two integers into a single integer. Constraints: y must be less than 65536.
    */
  def encode(x: Int, y: Int): Int = {
    // Shift x to the left to make room, then combine with y using OR
    (x << SHIFT_SIZE) | y
  }

  /** Unpacks the single integer back into a tuple (x, y).
    */
  def decode(z: Int): (Int, Int) = {
    // Shift right to bring x back down
    val x = z >> SHIFT_SIZE

    // Use bitwise AND to wipe out x and leave only the lower bits (y)
    val y = z & Y_MASK

    (x, y)
  }

  // --- Usage Example ---
  def main(args: Array[String]): Unit = {
    val xOriginal = 9999
    val yOriginal = 1999

    val encoded = encode(xOriginal, yOriginal)
    val decoded = decode(encoded)

    println(s"Original: ($xOriginal, $yOriginal)")
    println(s"Encoded z: $encoded") // Result will be 655356463
    println(s"Decoded: ${decoded._1}, ${decoded._2}")
  }
}
