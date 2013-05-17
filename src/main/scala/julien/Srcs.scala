package julien

import galago.core.util.ExtentArray

/** A trait that indicates [[CountStatistics]] can be provided. */
trait StatisticsSrc { def statistics: CountStatistics }

/** A trait that indicates Boolean matches can be provided. */
trait BoolSrc { def isMatch(id: InternalId): Boolean }

/** A trait that indicates a count can be provided. */
trait CountSrc { def count(id: InternalId): Int }

/** A trait that indicates a length can be provided. */
trait LengthsSrc { def length(id: InternalId): Int }

/** A trait that indicates [[Positions]] can be provided. */
trait PositionSrc { def positions(id: InternalId): ExtentArray }

/** A trait that indicates a generic type T can be provided. */
trait DataSrc[T] { def data(id: InternalId): T }

/** A trait that indicates a score can be provided. */
trait ScoreSrc { def score(id: InternalId): Double }
