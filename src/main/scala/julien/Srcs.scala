package julien

/** A trait that indicates [[CountStatistics]] can be provided. */
trait StatisticsSrc { def statistics: CountStatistics }

/** A trait that indicates Boolean matches can be provided. */
trait BoolSrc { def isMatch: Boolean }

/** A trait that indicates a [[Count]] can be provided. */
trait CountSrc { def count: Count }

/** A trait that indicates a [[Length]] can be provided. */
trait LengthsSrc { def length: Length }

/** A trait that indicates [[Positions]] can be provided. */
trait PositionSrc { def positions: Positions }

/** A trait that indicates a generic type T can be provided. */
trait DataSrc[T] { def data: T }

/** A trait that indicates a [[Score]] can be provided. */
trait ScoreSrc { def score: Score }
