package julien

/** A specialized Tuple for a scored document.
  * Holds the id of the document and the score.
  */
case class ScoredDocument(docid: Docid, score: Score)
// TODO: Would like to generalize this into a collection type.
//       It's rare to have a scored document in isolation.
