package lightdb.util

import java.security.SecureRandom
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

  private lazy val secure = new SecureRandom

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
   * True if randomization should be secure. Defaults to false.
   */
  var defaultSecure: Boolean = false

  /**
   * Uses java.util.concurrent.ThreadLocalRandom to generate random numbers.
   *
   * @param max the maximum value to include
   * @return random number between 0 and max
   */
  final def threadLocalRandom(max: Int): Int = ThreadLocalRandom.current().nextInt(max)

  final def secureRandom(max: Int): Int = synchronized {
    secure.nextInt(max)
  }

  /**
   * Generates a unique String using the characters supplied at the length defined.
   *
   * @param length     the length of the resulting String. Defaults to Unique.defaultLength.
   * @param characters the characters for use in the String. Defaults to Unique.defaultCharacters.
   * @param secure     true if the randomization should be secure. Defaults to Unique.defaultSecure.
   * @return a unique String
   */
  def apply(length: Int = defaultLength, characters: String = defaultCharacters, secure: Boolean = defaultSecure): String = {
    val charMax = characters.length
    val r = if (secure) secureRandom _ else random
    (0 until length).map(i => characters.charAt(r(charMax))).mkString
  }

  /**
   * Convenience functionality to generate a UUID (https://en.wikipedia.org/wiki/Universally_unique_identifier)
   *
   * 32 characters of unique hexadecimal values with dashes representing 36 total characters
   */
  def uuid(secure: Boolean = false): String = {
    val a = apply(8, Hexadecimal, secure)
    val b = apply(4, Hexadecimal, secure)
    val c = apply(3, Hexadecimal, secure)
    val d = apply(1, "89ab", secure)
    val e = apply(3, Hexadecimal, secure)
    val f = apply(12, Hexadecimal, secure)
    s"$a-$b-4$c-$d$e-$f"
  }

  /**
   * Returns the number of possible values for a specific length and characters.
   */
  def poolSize(length: Int = 32, characters: String = AllLettersAndNumbers): Long = {
    math.pow(characters.length, length).toLong
  }
}