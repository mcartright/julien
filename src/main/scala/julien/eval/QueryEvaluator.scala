package julien
package eval

object Metric {
  def expectedReciprocalRank(rankCutoff: Int) = { new ExpectedReciprocalRank(rankCutoff)}

  def normalizedDiscountCumulativeGain(documentsRetrieved:Int) = { new NormalizedDiscountedCumulativeGain(documentsRetrieved)}
  def meanReciprocalRank = { new MeanReciprocalRank}
  def bPref = { new BinaryPreference }
  def rPrecision = { new RPrecision}
  def meanAveragePrecision = { new MeanAveragePrecision}
  def relevantRetrieved  = { new CountRelevantRetrieved}
  def numRelevant  = new CountRelevant
  def numRetrieved = { new CountRetrieved}
  def precision(rankCutoff : Int) = { new Precision(rankCutoff)}

}

abstract class QueryEvaluator() {

  val name:String

  def eval[T <: ScoredObject[T]](
    result: QueryResult[T],
    judgment: QueryJudgments,
    strictlyEval: Boolean = true): Double

  // TODO
  // Need a natural reducer for this - how to provide?
  def eval[T <: ScoredObject[T]](
    results: QueryResultSet[T],
    judgments: QueryJudgmentSet
  ): Double = {
    var sum = 0.0
    for ((query, result) <- results) {
    //  assume(judgments.contains(query), s"Judgments missing query $query")
      sum += eval(result, judgments(query))
    }
    sum
  }

  def numRelevant(judgments : QueryJudgments) : Int = {
    val numRel = judgments.values.count(_.label > 0)
    numRel
  }

  def numNonRelevant(judgments : QueryJudgments) : Int = {
    val numRel = judgments.values.count(_.label <= 0)
    numRel
  }

  def isRelevant(judgment: RelevanceJudgment) : Boolean = {
    if (judgment.label > 0) true else false
  }

  def isRelevant(documentId : String, judgments : QueryJudgments) : Boolean = {
      val judgment = judgments.get(documentId)
    judgment match {
      case Some(j) => if (j.label > 0) true else false
      case _ => false // unknown case technically
    }
  }
}
