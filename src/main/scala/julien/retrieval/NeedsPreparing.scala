package julien
package retrieval

/** This is a marker trait that indicates the need for preparation
  * prior to execution. A typical example is an OrderedWindow view
  * needs to calculate some statistics before scoring takes place to
  * ensure accurate scoring.
  *
  * After all updates to statistics are made, the operator is told
  * that no more statistics are coming (via a call to 'prepared').
  *
  * If this needs generalization to different types of preparation,
  * I will probably add more traits to cover the different cases.
  */
trait NeedsPreparing {
  def updateStatistics(docid: Int): Unit
  protected var amIReady: Boolean = false
  def isPrepared: Boolean = amIReady
  def prepared: Unit = amIReady = true
}
