package julien

/** Holds a "score" for a particular term in the collection.
  * Useful for term-based expansion techniques.
  */
case class Gram(term: String, score: Double)
// TODO: Would like to generalize this to other expansion
//       types. Maybe it should be a collection type?
