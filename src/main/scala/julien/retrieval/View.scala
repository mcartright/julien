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

/** A view that must provide a score of a given index key for
  * a given identifier.
  */
trait ScoreView extends View with ScoreSrc

/** A view that must provide a aggregate statistics of a given index key for
  * a given identifier.
  */
trait StatisticsView extends View with StatisticsSrc

/** A view that must provide the positions of occurrences of a given index
  * key for a given identifier.
  */
trait PositionsView extends CountView with PositionSrc

/** A view that can provide both position information and statistics information
  * for a given identifier.
  */
trait PositionStatsView extends PositionsView with StatisticsView

/** A view that must provide for the number of index key occurrences in
  * a given identifier.
  */
trait LengthsView extends View with LengthsSrc

/** A view provides some generic data for a given index key for
  * a given identifier.
  */
trait DataView[T] extends View with DataSrc[T]
