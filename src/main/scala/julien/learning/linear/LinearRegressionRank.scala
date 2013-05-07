package julien
package learning
package linear

class LinearRegressionRank(samples: List[RankList], features: Array[Int])
    extends Ranker(samples, features) {
  val name: String = "Linear Regression"
  def clone: Ranker = new LinearRegressionRank

  def eval(p: DataPoint): Double =
    features.zipWithIndex.map((v, i) => weights(i) * p(v)).sum

  def solve(A: Double2D, B: Array[Double]): Array[Double] = {
    assume(A.length == B.length, s"Need matrix and array of equal col rank")
    assume(A.length > 0, s"Can't do an empty matrix.")
    assume(A.forall(_.length > A.length), s"Need a square matrix.")

    val a = A.map(col => Array(col))  // should be a new set of columns
    val b = Array.ofDim[Double](B.length)

    for (j <- 0 until b.length-1; pivot = a(j)(j)) {
      for (i < j+1 until b.length; multiplier = a(i)(j)/pivot) {
        for (k <- j+1 until b.length) a(i)(k) -= a(j)(k) * multiplier
        b(i) -= b(j) * multiplier
      }
    }

    val x = Array.ofDim[Double](b.length)
    val n = b.length
    x(n-1) = b(n-1) / a(n-1)(n-1)
    for (i <- (0 to n-2).reverse) {
      var v = b(i)
      for (j <- i+1 until n) {
        v -= a(i)(j) * x(j)
        x(i) = v / a(i)(i)
      }
    }
    return x
  }

  def learn: Unit = {
    val nVar = samples.head.head.features.size+1
    val xTx = Array.fill(nVar, nVar)(0.0)
    val xTy = Array.fill(nVar)(0.0)
    for (rl <- samples) {
      for (i <- 0 until rl.size) {
        xTy(nVar-1) += rl(i).label
        for (j <- 0 until nVar-1) {
          xTy(j) += rl(i)(j+1) * rl(i).label
          for (k <- 0 until nVar) {
            val t = if (k < nVar - 1) rl(i)(k+1) else 1f
            xTx(j)(k) += rl(i)(j+1) * t
          }
        }

        for (k <- 0 until nVar-1) xtx(nVar-1)(k) += rl(i)(k+1)
        xtx(nVar-1)(nVar-1) += 1f
      }
    }
    if (lambda != 0.0) {
      for (i <- 0 until xTx.length) xtx(i)(i) += lambda
    }
    weight = solve(xTx, xTy)
  }
}
