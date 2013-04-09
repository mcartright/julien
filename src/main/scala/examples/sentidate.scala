import edu.stanford.nlp.ie.AbstractSequenceClassifier
import edu.stanford.nlp.ie.crf._
import edu.stanford.nlp.io.IOUtils
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.ling.CoreAnnotations.AnswerAnnotation
import scala.collection.mutable.PriorityQueue
import org.lemurproject.galago.core.parse.{Document,Tag,TagTokenizer}
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.LinkedHashMap
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import scala.collection.immutable.Set
import scala.collection.Map

import java.net.URL
import scala.io.Source._
import java.io.IOException

object SDFeatures {
  def generate(
    qN: Node,
    nodeMap: Map[Node, java.lang.Object],
    index: Index) : List[FeatureFunction] = {

    // Stanford NER tagger
    val classifier =
      CRFClassifier.getClassifier(Classifiers.muc7)
    val dummy = new Parameters
    val feats = ListBuffer[FeatureFunction]()

    // Get the doc content and find dates - make the final score
    // dependent on the number of date-tagged terms (simple for now)
    var f = () => {
      val it = nodeMap(qN).asInstanceOf[TCI]
      val id = it.getContext.document
      val doc = index.getDocument(index.getName(id), dummy)
      val results: java.util.List[java.util.List[CoreLabel]] =
        classifier.classify(doc.text)
      val dateTagCount = results.flatten.filter {
        cl: CoreLabel =>
        cl.get[String, AnswerAnnotation](classOf[AnswerAnnotation]) ==
        "DATE"
      }.size
      dateTagCount.toDouble
    }
    feats += f

    // Now let's do some sentiment analysis


    feats.toList
  }
}

object SentiDate extends App {
  // components of our feature functions
  // Hold external references to the feature weights so we can tune quickly
  val weightTable = HashMap[String, List[Double]]()
  val queryNodes = bowNodes(query, "extents")
  val index = Sources.get('aquaint)
  val lengths = index.getLengthsIterator
  val nodeMap = LinkedHashMap[Node, java.lang.Object]()
  for (n <- queryNodes) { nodeMap.update(n,
    index.getIterator(n).asInstanceOf[TEI])
  }

  val iterators = nodeMap.values.toList.map(obj2movableIt(_))

  // Make the scorers - each one will have its own feature/weight vector
  val scorers = queryNodes.map { qN =>
    val scorer = dirichlet(collectionFrequency(qN, index, lengths))
    val features = SDFeatures.generate(qN, nodeMap, index)
    val weights = List.fill(features.size)(1.0)
    ParameterizedScorer(features, weights, scorer, lengths, nodeMap(qN))
  }

  // Scoring loop
  val resultQueue = standardScoringLoop(scorers, iterators, lengths)

  // Get doc names and print
  printResults(resultQueue, index)
}
