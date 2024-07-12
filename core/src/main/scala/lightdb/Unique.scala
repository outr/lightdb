package lightdb

import java.util.concurrent.ThreadLocalRandom

/**
 * Unique String generator
 */
object Unique {
  lazy val LettersLower = "abcdefghijklmnopqrstuvwxyz"
  lazy val LettersUpper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  lazy val Numbers = "0123456789"
  lazy val Readable = "ABCDEFGHJKLMNPQRSTWXYZ23456789"
  lazy val Hexadecimal = s"${Numbers}abcdef"
  lazy val LettersAndNumbers = s"$LettersLower$Numbers"
  lazy val AllLetters = s"$LettersLower$LettersUpper"
  lazy val AllLettersAndNumbers = s"$LettersLower$LettersUpper$Numbers"

  /**
   * Random number generator used to generate unique values. Defaults to `threadLocalRandom`.
   */
  var random: Int => Int = threadLocalRandom

  /**
   * The default length to use for generating unique values. Defaults to 32.
   */
  var defaultLength: Int = 32

  /**
   * The default characters to use for generating unique values. Defaults to AllLettersAndNumbers.
   */
  var defaultCharacters: String = AllLettersAndNumbers

  /**
   * Uses java.util.concurrent.ThreadLocalRandom to generate random numbers.
   *
   * @param max the maximum value to include
   * @return random number between 0 and max
   */
  final def threadLocalRandom(max: Int): Int = ThreadLocalRandom.current().nextInt(max)

  /**
   * Generates a unique String using the characters supplied at the length defined.
   *
   * @param length     the length of the resulting String. Defaults to Unique.defaultLength.
   * @param characters the characters for use in the String. Defaults to Unique.defaultCharacters.
   * @return a unique String
   */
  def apply(length: Int = defaultLength, characters: String = defaultCharacters): String = {
    val charMax = characters.length
    val r = random
    (0 until length).map(_ => characters.charAt(r(charMax))).mkString
  }

  /**
   * Convenience functionality to generate a UUID (https://en.wikipedia.org/wiki/Universally_unique_identifier)
   *
   * 32 characters of unique hexadecimal values with dashes representing 36 total characters
   */
  def uuid: String = {
    val a = apply(8, Hexadecimal)
    val b = apply(4, Hexadecimal)
    val c = apply(3, Hexadecimal)
    val d = apply(1, "89ab")
    val e = apply(3, Hexadecimal)
    val f = apply(12, Hexadecimal)
    s"$a-$b-4$c-$d$e-$f"
  }

  /**
   * Returns the number of possible values for a specific length and characters.
   */
  def poolSize(length: Int = 32, characters: String = AllLettersAndNumbers): Long = {
    math.pow(characters.length, length).toLong
  }
}