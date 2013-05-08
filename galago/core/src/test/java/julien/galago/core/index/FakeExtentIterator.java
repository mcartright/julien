// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index;

import java.io.IOException;

import julien.galago.core.index.ExtentIterator;
import julien.galago.core.index.Iterator;
import julien.galago.core.util.ExtentArray;
import julien.galago.tupleflow.Utility;


/**
 *
 * @author trevor
 * @author irmarc
 */
public class FakeExtentIterator implements ExtentIterator {

  private int[][] data;
  private int index;

  public FakeExtentIterator(int[][] data) {
    this.data = data;
    this.index = 0;
  }

  @Override
  public boolean isDone() {
    return index >= data.length;
  }

  @Override
  public int currentCandidate() {
    if (index < data.length) {
      return data[index][0];
    } else {
      return Integer.MAX_VALUE;
    }
  }

  @Override
  public int count() {
    return data[index].length - 1;
  }

  @Override
  public void reset() throws IOException {
    index = 0;
  }

  @Override
  public ExtentArray extents() {
    ExtentArray array = new ExtentArray();
    int[] datum = data[index];
    for (int i = 1; i < datum.length; i++) {
      array.add(datum[i]);
    }

    return array;
  }

  @Override
  public boolean hasMatch(int identifier) {
    if (isDone()) {
      return false;
    } else {
      return (currentCandidate() == identifier);
    }
  }

  @Override
  public void syncTo(int identifier) throws IOException {
    while (!isDone() && currentCandidate() < identifier) {
      index++;
    }
  }

  @Override
  public void movePast(int identifier) throws IOException {
    syncTo(identifier + 1);
  }

  @Override
  public long totalEntries() {
    return data.length;
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
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public int maximumCount() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean hasAllCandidates() {
    return false;
  }

  @Override
  public byte[] key() {
    return Utility.fromString("FAKE");
  }

  @Override
  public long sizeInBytes() throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
