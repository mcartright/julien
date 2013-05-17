// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index;

import java.io.IOException;

import julien.galago.tupleflow.Utility;

/**
 * Iterates over the a KeyIterator as if it were a Value iterator. Useful for
 * iterating over document lengths or document names.
 *
 * @author irmarc
 */
public abstract class KeyToListIterator implements Iterator {

  protected KeyIterator iterator;
  protected long size;

  public KeyToListIterator(KeyIterator ki) {
    iterator = ki;
    try {
      size = ki.getValueLength();
    } catch(Exception e) {
      size = Long.MAX_VALUE;
    }
  }
  
  @Override
  public long sizeInBytes() {
    return size;
  }

  @Override
  public boolean syncTo(int identifier) throws IOException {
    iterator.skipToKey(Utility.fromInt(identifier));
    return hasMatch(identifier);
  }

  @Override
  public int movePast(int identifier) throws IOException {
    iterator.skipToKey(Utility.fromInt(identifier + 1));
    if (!isDone()) {
      return Utility.toInt(iterator.getKey());
    } else {
      return Integer.MAX_VALUE;
    }
  }

  @Override
  public void reset() throws IOException {
    iterator.reset();
  }

  @Override
  public boolean isDone() {
    return iterator.isDone();
  }

  @Override
  public int currentCandidate() {
    try {
      return Utility.toInt(iterator.getKey());
    } catch (IOException ioe) {
      return Integer.MAX_VALUE;
    }
  }

  @Override
  public boolean hasMatch(int identifier) {
    return (!isDone() && currentCandidate() == identifier);
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
}
