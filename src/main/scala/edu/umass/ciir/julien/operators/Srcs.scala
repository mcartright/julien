package operators

sealed trait CountSrc { def count: Count }
sealed trait PositionSrc { def positions: Positions }
sealed trait DataSrc[T] { def data: T }
sealed trait ScoreSrc { def score: Score }
