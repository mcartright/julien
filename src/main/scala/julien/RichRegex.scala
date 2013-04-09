package julien

import scala.util.matching.Regex

/** Implicit extension to the Regex class (done via composition)
  * this class provides easy "match" and "miss" type methods against
  * regular Strings. Only restriction is that the RichRegex must come
  * first in the comparison. See examples in the methods below.
  */
implicit class RichRegex(underlying: Regex) {
  // TODO: Add some memoization of the underlying patterns. Should be done
  //       in the object, I imagine, as they should be shared once compiled.

  /** Returns true if the given string matches this pattern. Example:
    *
    * """\d+""".r matches "90210"  => True
    */
  def matches(s: String): Boolean = underlying.pattern.matcher(s).matches

  /**
    * @see #matches(s: String)
    */
  def ==(s: String): Boolean = matches(s)

  /** Logical negation of #matches(s: String).
    */
  def misses(s: String): Boolean = (matches(s) == false)

  /**
    * @see #misses(s: String)
    */
  def !=(s: String): Boolean = misses(s)
}
