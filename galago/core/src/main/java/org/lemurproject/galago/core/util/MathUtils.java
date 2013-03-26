// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.util;

import java.util.Arrays;

/**
 * Supplements Java's sucky math class.
 *
 * @author irmarc
 */
public class MathUtils {

  // Can't instantiate Math. It's ALWAYS there.
  private MathUtils() {
  }

  public static double max(double[] numbers) {
    assert numbers != null;
    assert numbers.length > 0;

    double maxSoFar = numbers[0];
    for (int i = 1; i < numbers.length; ++i) {
      if (numbers[i] > maxSoFar) {
        maxSoFar = numbers[i];
      }
    }
    return maxSoFar;
  }

  public static double max(int[] numbers) {
    assert numbers != null;
    assert numbers.length > 0;

    double maxSoFar = numbers[0];
    for (int i = 1; i < numbers.length; ++i) {
      if (numbers[i] > maxSoFar) {
        maxSoFar = numbers[i];
      }
    }
    return maxSoFar;
  }

  public static double min(double[] numbers) {
    assert numbers != null;
    assert numbers.length > 0;

    double minSoFar = numbers[0];
    for (int i = 1; i < numbers.length; ++i) {
      if (numbers[i] < minSoFar) {
        minSoFar = numbers[i];
      }
    }
    return minSoFar;
  }

  public static double min(int[] numbers) {
    assert numbers != null;
    assert numbers.length > 0;

    double minSoFar = numbers[0];
    for (int i = 1; i < numbers.length; ++i) {
      if (numbers[i] > minSoFar) {
        minSoFar = numbers[i];
      }
    }
    return minSoFar;
  }

  public static double updatedArithmeticMean(double priorMean, double numPriorSamples, double sampleValue) {
    return ((priorMean * numPriorSamples) + sampleValue) / (numPriorSamples + 1);
  }

  public static double updatedVariance(double priorMean, double newMean, double priorVariance,
          double sampleValue) {
    return priorVariance + ((sampleValue - priorMean) * (sampleValue - newMean));
  }

  public static double arithmeticMean(double[] numbers) {
    assert numbers != null;
    assert numbers.length > 0;

    double total = 0;
    for (double d : numbers) {
      total += d;
    }
    return total / numbers.length;
  }

  public static double variance(double[] numbers) {
    assert numbers != null;
    assert numbers.length > 0;

    double m = arithmeticMean(numbers);
    double[] differences = new double[numbers.length];
    for (int i = 0; i < numbers.length; ++i) {
      differences[i] = Math.pow(m - numbers[i], 2.0);
    }
    return arithmeticMean(differences);
  }

  public static double skewness(double[] numbers) {
    assert numbers != null;
    assert numbers.length > 0;

    double m = arithmeticMean(numbers);
    double stddev = Math.sqrt(variance(numbers));

    double[] moments = new double[numbers.length];
    for (int i = 0; i < numbers.length; ++i) {
      moments[i] = Math.pow((numbers[i] - m) / stddev, 3.0);
    }
    return arithmeticMean(moments);
  }

  public static double kurtosis(double[] numbers) {
    assert numbers != null;
    assert numbers.length > 0;

    double m = arithmeticMean(numbers);

    double[] numeratorNumbers = new double[numbers.length];
    double[] denominatorNumbers = new double[numbers.length];

    for (int i = 0; i < numbers.length; ++i) {
      double diff = numbers[i] - m;
      numeratorNumbers[i] = Math.pow(diff, 4.0);
      denominatorNumbers[i] = Math.pow(diff, 2.0);
    }
    double numerator = arithmeticMean(numeratorNumbers);
    double denominator = Math.pow(arithmeticMean(denominatorNumbers), 2.0);
    return numerator / denominator;
  }

  // Can't believe I'm implementing this.
  // multiplicative form from Wikipedia -- irmarc
  public static long binomialCoeff(int n, int k) {
    if (n <= k) {
      return 1;
    }
    int c;
    if (k > n - k) { // take advantage of symmetry
      k = n - k;
    }
    c = 1;
    for (int i = 0; i < k; i++) {
      c *= (n - i);
      c /= (i + 1);

    }
    return c;
  }

  public static double logSumExp(double[] scores) {
    double[] weights = new double[scores.length];
    Arrays.fill(weights, 1.0);
    return weightedLogSumExp(weights, scores);
  }

  /**
   * Computes the weighted average of scores: -> log( w0 * exp(score[0]) + w1 *
   * exp(score[1]) + w1 * exp(score[2]) + .. )
   *
   * to avoid rounding errors, we compute the equivalent expression:
   *
   * returns: maxScore + log( w0 * exp(score[0] - max) + w1 * exp(score[1] -
   * max) + w2 * exp(score[2] - max) + .. )
   */
  public static double weightedLogSumExp(double[] weights, double[] scores) {
    if (scores.length == 0) {
      throw new IllegalArgumentException("weightedLogSumExp was called with a zero length array of scores.");
    }

    // find max value - this score will dominate the final score
    double max = Double.NEGATIVE_INFINITY;
    for (int i = 0; i < scores.length; i++) {
      max = Math.max(scores[i], max);
    }

    double sum = 0;
    for (int i = 0; i < scores.length; i++) {
      sum += weights[i] * java.lang.Math.exp(scores[i] - max);
    }
    sum = max + java.lang.Math.log(sum);

    return sum;
  }
}
