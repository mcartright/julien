package julien
package eval

import gnu.trove.map.hash.TObjectIntHashMap
import gnu.trove.iterator.TObjectIntIterator
import scala.collection.Iterable


/** The label of a given document. These are only generated
  * by [[julien.eval.QueryJudgments QueryJudgments]], which implicitly
  * provides the query for which this judgment applies.
  */
case class Judgment(var name: String, var label: Int, var isRelevant: Boolean)


object QueryJudgments {
  // Default to this for now
  def apply(qid: String): QueryJudgments = new BinaryJudgments(qid)
}

/** A set of judgments for a single query. Logically, a QueryJudgment is
  * a mapping of `query -> Map[Docid, Label]`. However this alone is
  * not enough to assert relevance. Subclass in order to provide a
  * definition of relevance.
  */
abstract class QueryJudgments(val queryName: String)
    extends Iterable[Judgment] {
  protected val judgments = new TObjectIntHashMap[String]()

  def update(docName: String, label: Int): this.type = put(docName, label)
  def put(docName: String, label: Int): this.type = {
    assume(!judgments.containsKey(docName), s"Got $docName already.")
    judgments.put(docName, label)
    return this
  }

  /** Counts the number of judgments that are relevant according to the
    * provided definition.
    */
  def numRelevant: Int = this.foldLeft(0) { (sum, j) =>
    if (j.isRelevant) sum + 1 else sum
  }

  /** Counts the number of judgments that are not relevant according to
    * the provided definition. If this is not a strict logical inversion
    * of relevance, this method should be overriden.
    */
  def numNonRelevant: Int = size - numRelevant

  def isRelevant(name: String): Boolean

  /** Defaults to the logical opposite of `isRelevant`. Override for different
    * behavior.
    */
  def isNonRelevant(name: String): Boolean = !isRelevant(name)

  /** Defaults to the logical opposite of `contains`. Override for different
    * behavior.
    */
  def isUnknown(name: String): Boolean = !contains(name)

  def isJudged(name: String): Boolean = contains(name)
  def contains(name: String): Boolean = judgments.containsKey(name)
  def get(name: String): Int = apply(name)
  def apply(name: String): Int = {
    assume(judgments.containsKey(name), s"$name is unjudged")
    judgments.get(name)
  }

  def getOrElse(name: String, default: Int): Int =
    if (judgments.containsKey(name)) judgments.get(name) else default

  def keys: Array[String] = judgments.keys.asInstanceOf[Array[String]]
  def values: Array[Int] = judgments.values
  override def size: Int = judgments.size

  def iterator: Iterator[Judgment] = new Iterator[Judgment] {
    val j = Judgment("", 0, false)
    var idx = 0
    val k = keys
    def hasNext: Boolean = idx < keys.length-1
    def next: Judgment = {
      j.name = keys(idx)
      j.label = judgments.get(j.name)
      j.isRelevant = isRelevant(j.name)
      idx += 1
      j
    }
  }

  /** Returns the number of preference pairs (i.e. positions i and j
    * such that i < j and label(i) > label(j))
    * for this query judgment.
    */
  def numPrefPairs: Int = {
    val b = Array.newBuilder[Judgment]
    val it = judgments.iterator
    while (it.hasNext) {
      it.advance
      b += Judgment(it.key, it.value, isRelevant(it.key))
    }
    val jlist = b.result
    var sum = 0
      for (
        i <- 0 until jlist.length-1;
        j <- i+1 until jlist.length;
        if jlist(i).label > jlist(j).label
      ) sum += 1
    sum
  }

  /** Does `first` have a lower value than `second`? */
  def isWorse(first: String, second: String): Boolean = isBetter(second, first)

  /** Does `first` have a higher value than `second`? */
  def isBetter(first: String, second: String): Boolean =
    judgments.get(first) > judgments.get(second)
}
