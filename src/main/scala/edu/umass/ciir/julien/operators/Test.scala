package operators

import scala.collection.mutable.PriorityQueue
import edu.umass.ciir.julien.Utils._
import edu.umass.ciir.julien.Aliases._
import edu.umass.ciir.julien.{ScoredDocument,ScoredDocumentOrdering}
import scala.collection.JavaConversions._

class IndexBasedDocument(
  index: Index,
  lIter: MLI)
    extends Document {
  type GDoc = org.lemurproject.galago.core.parse.Document

  private def doc: GDoc =
    index.document(index.underlying.getName(lIter.getCurrentIdentifier))
  def length = new Length(lIter.getCurrentLength)
  def count(op: CountOp) = op.count
  def positions(op: PositionsOp) = op.positions
  def content: String = doc.text

  // These depend on the term vector being present
  def vocabulary: Set[String] = doc.terms.toSet
  def histogram: Map[String, Int] =
    doc.terms.groupBy { case s => s }.mapValues(_.size)
  def multinomial: Map[String, Double] = {
    val h = doc.terms.groupBy { case s => s }.mapValues(_.size)
    val sum = h.values.sum
    h.mapValues(_.toDouble / sum)
  }
}

object Test {
  implicit def term2op(t: Term): SingleTermOp = SingleTermOp(t)
  type TMap = Map[Operator, Set[Term]]

  def main(args: Array[String]): Unit = {
    val query = "united states of america".split(" ").map(Term(_))
    val sdm =
      Combine(
        Weight(Combine(query.map(a => Dirichlet(a)): _*), 0.8),
        Weight(Combine(query.sliding(2,1).map { p =>
          Dirichlet(OrderedWindow(1, p: _*))
        }.toSeq: _*), 0.15),
        Weight(Combine(query.sliding(2,1).map { p =>
          Dirichlet(UnorderedWindow(8, p: _*))
        }.toSeq: _*), 0.05)
      )
    val tMap = findTerms(sdm)
    for ((k,v) <- tMap) {
      Console.printf("Node %s --> %s\n", k, v.mkString(","))
    }

    // Assign iterators to each of the underlying terms
    val index = Index.disk(args(0))
    val termObjs = tMap.values.reduce((a,b) => a ++ b)
    termObjs.foreach( term => term.underlying = index.iterator(term.t))
    val iterators = termObjs.map(_.underlying)
    val lengths = index.lengthsIterator
    val scorers : List[FeatureOp] = List[FeatureOp](sdm)
    val currentdoc: Document = new IndexBasedDocument(index, lengths)
    // Go
    val numResults: Int = 100
    val resultQueue = PriorityQueue[ScoredDocument]()(ScoredDocumentOrdering)
    while (iterators.exists(_.isDone == false)) {
      val candidate = iterators.filterNot(_.isDone).map(_.currentCandidate).min
      lengths.syncTo(candidate)
      iterators.foreach(_.syncTo(candidate))
      if (iterators.exists(_.hasMatch(candidate))) {
        // Time to score
        var score = scorers.foldRight(new Score(0.0)) { (S,N) =>
          S match {
            case i: IntrinsicEvaluator => i.eval + N
            case t: TraversableEvaluator[Document] => t.eval(currentdoc) + N
            case _ => N
          }
        }
        resultQueue.enqueue(ScoredDocument(candidate, score.underlying))
        if (resultQueue.size > numResults) resultQueue.dequeue
      }
      iterators.foreach(_.movePast(candidate))
    }
    printResults(resultQueue.reverse, index.underlying)
  }

  def findTerms(
    root: Operator,
    ctx: List[Operator] = List[Operator](),
    m: TMap = Map[Operator, Set[Term]]()) : TMap =
    root match {
      case s: SingleTermOp => ctx.foldLeft(m) {
        (m, o) => m + (o -> (m.getOrElse(o, Set[Term]()) + s.t))
      }
      case ow: OrderedWindow => ctx.foldLeft(m) {
        (m2, o) => ow.terms.foldLeft(m2) {
          (m3, t) => m3 + (o -> (m3.getOrElse(o, Set[Term]()) + t))
        }
      }
      case uw: UnorderedWindow => ctx.foldLeft(m) {
        (m2, o) => uw.terms.foldLeft(m2) {
          (m3, t) => m3 + (o -> (m3.getOrElse(o, Set[Term]()) + t))
        }
      }
      case w: Weight => m ++ findTerms(w.op, root :: ctx, m)
      case c: Combine => c.ops.foldLeft(m) { (m, o) =>
        findTerms(o, root :: ctx, m)
      }
      case d: Dirichlet => m ++ findTerms(d.op, root :: ctx, m)
      case _ => m
    }
}
