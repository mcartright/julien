// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Base class for any data structures that map a key value to a list of data,
 * where one cannot assume the list can be held in memory
 *
 *
 * @author irmarc
 */
public abstract class KeyListReader extends KeyValueReader {

  public KeyListReader(String filename) throws FileNotFoundException, IOException {
    super(filename);
  }

  public KeyListReader(BTreeReader r) {
    super(r);
  }

  public abstract class ListIterator implements Iterator {

    // OPTIONS
    public static final int HAS_SKIPS = 0x01;
    public static final int HAS_MAXTF = 0x02;
    public static final int HAS_INLINING = 0x04;
    protected byte[] key;

    public ListIterator(byte[] key) {
      this.key = key;
    }

    public abstract void reset(BTreeReader.BTreeIterator it) throws IOException;

    @Override
    public byte[] key() {
      return key;
    }

    @Override
    public boolean hasMatch(int id) {
      return (!isDone() && currentCandidate() == id);
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
}
