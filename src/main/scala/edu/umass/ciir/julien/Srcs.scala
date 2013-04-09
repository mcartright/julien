package edu.umass.ciir.julien

trait StatisticsSrc { def statistics: CountStatistics }
trait BoolSrc { def isMatch: Boolean }
trait CountSrc { def count: Count }
trait LengthsSrc { def length: Length }
trait PositionSrc { def positions: Positions }
trait DataSrc[T] { def data: T }
trait ScoreSrc { def score: Score }
