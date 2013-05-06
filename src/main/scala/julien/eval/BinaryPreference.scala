package julien
package eval

/** I do NOT trust the current implementation of this class, nor do I trust
  * the version in Galago. This needs a code review against the 2006 corrected
  * version of this paper.
  */
class BinaryPreference(n: String) extends QueryEvaluator(n) {
  def eval[T <: ScoredObject[T]](
    result: QueryResult[T],
    judgment: QueryJudgment,
    strictlyEval: Boolean): Double = {
    val totalRel = judgment.numRel
    if (totalRel == 0)
      if (strictlyEval)
        throw new Exception(s"No relevant docs for query")
      else return 0

    val nonRelCount =
      if (totalRel > judgment.numNonRel) judgment.numNonRel else totalRel
    val (judged, unjudged) = result.partition { so =>
      judgment.contains(so.name)
    }
    val (relevant , nonrelevant) = judged.partition { so =>
      judgment.isRelevant(so.name)
    }

    var sum = if (nonrelevant.size == 0) relevant.size.toDouble else 0.0
    var i, j = 0
    while (i < relevant.size && j < nonrelevant.size) {
      val rel = relevant(i)
      val irr = nonrelevant(j)
      if (rel.rank < irr.rank) {
        assume (j <= totalRel)
        sum += 1.0 - (j.toDouble / nonRelCount)
        i += 1
      } else j += 1
    }
    sum / totalRel.toDouble
  }
}
