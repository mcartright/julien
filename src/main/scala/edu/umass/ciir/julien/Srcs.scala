package edu.umass.ciir.julien

trait StatisticsSrc { def statistics: CountStatistics }
trait CountSrc { def count: Count }
trait PositionSrc { def positions: Positions }
trait DataSrc[T] { def data: T }
trait ScoreSrc { def score: Score }
