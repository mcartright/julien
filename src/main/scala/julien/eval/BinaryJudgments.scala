package julien
package eval

/** Completes the definition of a [[julien.eval.QueryJudgments QueryJudgments]]
  * by binarizing labels into the following category:
  * - <= 0 is non-relevant
  * - > 0 is relevant.
  */
class BinaryJudgments(q: String) extends QueryJudgments(q) {
  def isRelevant(name: String): Boolean =
    judgments.containsKey(name) && judgments.get(name) > 0
}
