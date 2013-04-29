// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index;

import java.io.IOException;

import julien.galago.core.index.Iterator;
import julien.galago.core.index.ScoreIterator;


/**
 *
 * @author trevor
 */
public class FakeScoreIterator implements ScoreIterator {

  int[] docs;
  double[] scores;
  double defaultScore;
  int index;

  public FakeScoreIterator(int[] docs, double[] scores) {
    this(docs, scores, 0);
  }

  public FakeScoreIterator(int[] docs, double[] scores, double defaultScore) {
    this.docs = docs;
    this.scores = scores;
    this.index = 0;
    this.defaultScore = defaultScore;
  }

  @Override
  public int currentCandidate() {
    if (index < docs.length) {
      return docs[index];
    } else {
      return Integer.MAX_VALUE;
    }

  }

  @Override
  public boolean hasMatch(int document) {
    if (isDone()) {
      return false;
    } else {
      return document == docs[index];
    }
  }

  @Override
  public void syncTo(int document) throws IOException {
    while (!isDone() && document > docs[index]) {
      index++;
    }
  }

  @Override
  public void movePast(int document) throws IOException {
    while (!isDone() && document >= docs[index]) {
      index++;
    }
  }

  @Override
  public double score() {
    return scores[index];
  }

  @Override
  public boolean isDone() {
    return (index >= docs.length);
  }

  @Override
  public void reset() {
    index = 0;
  }

  @Override
  public double maximumScore() {
    return Double.POSITIVE_INFINITY;
  }

  @Override
  public double minimumScore() {
    return Double.NEGATIVE_INFINITY;
  }

  @Override
  public long totalEntries() {
    return docs.length;
  }

  @Override
  public int compareTo(Iterator other) {
    if (isDone() && !other.isDone()) {
      return 1;
    }
    if (other.isDone() && !isDone()) {
      return -1;
    }
    if (isDone() && other.isDone()) {
      return 0;
    }
    return currentCandidate() - other.currentCandidate();
  }

  @Override
  public String getEntry() throws IOException {
    return currentCandidate() + "," + score();
  }

  @Override
  public boolean hasAllCandidates() {
    return false;
  }

  @Override
  public void setMaximumScore(double newMax) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void setMinimumScore(double newMin) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public byte[] key() {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
