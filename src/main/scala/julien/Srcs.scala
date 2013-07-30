package julien

import galago.core.util.ExtentArray

/** A trait that indicates [[CountStatistics]] can be provided. */
trait StatisticsSrc { def statistics: CountStatistics }

/** A trait that indicates Boolean matches can be provided. */
trait BoolSrc { def isMatch(id: Int): Boolean }

/** A trait that indicates a count can be provided. */
trait CountSrc { def count(id: Int): Int }

/** A trait that indicates a length can be provided. */
trait LengthsSrc { def length(id: Int): Int }

/** A trait that indicates positions (as `ExtentArray`s) can be provided. */
trait PositionSrc { def positions(id: Int): ExtentArray }

/** A trait that indicates a generic type T can be provided. */
trait DataSrc[T] { def data(id: Int): T }

/** A trait that indicates a score can be provided. */
trait ScoreSrc { def score(id: Int): Double }
