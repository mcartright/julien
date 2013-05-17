/*
 * BSD License (http://www.galagosearch.org/license)

 */
package julien.galago.core.index;

import java.io.IOException;

import julien.galago.core.index.LengthsReader.LengthsIterator;

/**
 *
 * @author marc
 */
public class FakeLengthIterator implements LengthsIterator {

  private int[] ids;
  private int[] lengths;
  private int position;

  public FakeLengthIterator(int[] i, int[] l) {
    ids = i;
    lengths = l;
    position = 0;
  }

  @Override
  public int getCurrentLength() {
    return lengths[position];
  }

  @Override
  public int getCurrentIdentifier() {
    return ids[position];
  }

  @Override
  public int currentCandidate() {
    if (isDone()) {
      return Integer.MAX_VALUE;
    } else {
      return ids[position];
    }
  }

  @Override
  public boolean hasMatch(int identifier) {
    return (ids[position] == identifier);
  }

  @Override
  public boolean hasAllCandidates() {
    return true;
  }

  @Override
  public int movePast(int identifier) throws IOException {
    syncTo(identifier + 1);
    return currentCandidate();
  }

  @Override
  public boolean syncTo(int identifier) throws IOException {
    while (!isDone() && ids[position] < identifier) {
      position++;
    }
    return currentCandidate() == identifier;
  }

  @Override
  public void reset() throws IOException {
    position = 0;
  }

  @Override
  public boolean isDone() {
    return (position >= ids.length);
  }

  @Override
  public String getEntry() throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public long totalEntries() {
    return ids.length;
  }

  @Override
  public int compareTo(Iterator t) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public byte[] getRegionBytes() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public byte[] key() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public long sizeInBytes() throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
