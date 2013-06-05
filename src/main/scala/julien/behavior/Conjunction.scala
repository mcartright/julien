package julien
package behavior

/** This trait indicates that the operator
  * contains children, but it requires all children to have
  * non-degenerate scores for a requested document in order for the
  * operator itself not to have a degenerate score.
  *
  * Without using this marker trait, all operators with children
  * are assumed to be disjunctive, which means the candidates of this
  * operator are the union of the children candidates. Many more candidates
  * will be scored, but the resulting ranked list will be the same.
  *
  */
trait Conjunction
