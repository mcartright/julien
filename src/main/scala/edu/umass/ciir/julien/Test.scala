package edu.umass.ciir.julien

import scala.collection.mutable.PriorityQueue
import edu.umass.ciir.julien.Utils._
import edu.umass.ciir.julien.Aliases._
import scala.collection.JavaConversions._

import org.lemurproject.galago.tupleflow.Parameters

object Test {
  implicit def term2op(t: Term): SingleTermOp = SingleTermOp(t)
  type TMap = Map[Operator, Set[Term]]

  def main(args: Array[String]): Unit = {
    val params = new Parameters(args)
    val query = params.getString("query").split(" ").map(Term(_))
    val ql = Combine(query.map(a => Dirichlet(a)): _*)
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

    // Assign iterators to each of the underlying terms
    val indexType = params.getString("type")
    assert("""memory|disk""".r matches indexType,
      s"Unknown index type: $indexType")
    val index : Index = indexType match {
      case "disk" => Index.disk(params.getString("disk"))
      case "memory" => Index.memory(params.getString("memory"))
    }
    // Connect to this index
    index.attach(ql)
    // extract iteators
    val iterators = ql.filter(_.isInstanceOf[Term]).map { t =>
      t.asInstanceOf[Term].underlying
    }
    val lengths = index.lengthsIterator
    val scorers : List[FeatureOp] = List[FeatureOp](sdm)

    // Go
    val numResults: Int = 100
    val resultQueue = PriorityQueue[ScoredDocument]()(ScoredDocumentOrdering)
    while (iterators.exists(_.isDone == false)) {
      val candidate = iterators.filterNot(_.isDone).map(_.currentCandidate).min
      lengths.syncTo(candidate)
      iterators.foreach(_.syncTo(candidate))
      if (iterators.exists(_.hasMatch(candidate))) {
        // Time to score
        lazy val currentdoc =
          index.document(index.underlying.getName(candidate))
        val len = new Length(lengths.getCurrentLength)
        var score = scorers.foldRight(new Score(0.0)) { (S,N) =>
          S match {
            case i: IntrinsicEvaluator => i.eval + N
            case l: LengthsEvaluator => l.eval(len) + N
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
