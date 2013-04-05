package factors

abstract class TermScoringFunction

case class DirichletParameters(cf: Double, cl: Double, mu : Double = 1500.0)
class DirichletScorer(parameters : DirichletParameters) extends TermScoringFunction {
  val background : Double = parameters.cf match {
    case 0 => 0.5 / parameters.cl
    case _ => parameters.cf / parameters.cl
  }

  def score(count: Int, length: Int) : Double = {
    val numerator = count + (parameters.mu * background)
    val denominator = length + parameters.mu
    return scala.math.log(numerator / denominator)
  }
}

case class JelinekMercerParameters(cf: Double, cl: Double, lambda: Double = 0.5)
class JelinekMercerScorer(parameters: JelinekMercerParameters) extends TermScoringFunction {
  val background : Double = parameters.cf match {
    case 0 => 0.5 / parameters.cl
    case _ => parameters.cf / parameters.cl
  }

  def score(count: Int, length: Int) : Double = {
    val foreground = count.toDouble / length
    return scala.math.log((parameters.lambda * foreground) + ((1-parameters.lambda)*background))
  }
}

case class BM25Parameters(cl: Double, df: Double, dc: Double, b: Double = 0.75, k: Double = 1.2)
class BM25Scorer(parameters: BM25Parameters) extends TermScoringFunction {
  val avgDocLength = parameters.cl / parameters.dc
  val idf = scala.math.log(parameters.dc / (parameters.df + 0.5))

  def score(count: Int, length: Int) : Double = {
    val numerator = count * (parameters.k + 1)
    val denominator = count + (parameters.k * (1 - parameters.b + (parameters.b * length / avgDocLength)))
    return idf * numerator / denominator
  }
}
