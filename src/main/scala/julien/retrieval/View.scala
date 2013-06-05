package julien
package retrieval

/** A view is an operator that provides information directly stored in an index
  * or other lookup structure. In some cases, a View may provide a score,
  * to maintain separation, this score must be filtered through a
  * [[julien.retrieval.Feature Feature]].
 */
trait View extends Operator

/** A view that must provide Boolean values for a given identifier.
  */
trait BooleanView extends View with BoolSrc

/** A view that must provide a count of occurrences of a given index key for
  * a given identifier.
  */
trait CountView extends View with CountSrc
trait StatisticsView extends View with StatisticsSrc
trait PositionsView extends CountView with PositionSrc
trait PositionStatsView extends PositionsView with StatisticsView
trait LengthsView extends View with LengthsSrc
trait DataView[T] extends View with DataSrc[T]
