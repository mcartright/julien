/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.lemurproject.galago.core.index.LengthsReader;
import org.lemurproject.galago.core.index.LengthsReader.LengthsIterator;

/**
 *
 * @author sjh
 */
public class DisjointLengthsIterator extends DisjointIndexesIterator implements LengthsIterator {

  public DisjointLengthsIterator(Collection<LengthsReader.LengthsIterator> iterators) {
    super((Collection) iterators);
  }

  @Override
  public int getCurrentLength() {
    if (head != null) {
      return ((LengthsReader.LengthsIterator) this.head).getCurrentLength();
    } else {
      throw new RuntimeException("Lengths Iterator is done.");
    }
  }

  @Override
  public int getCurrentIdentifier() {
    if (head != null) {
      return ((LengthsReader.LengthsIterator) this.head).getCurrentIdentifier();
    } else {
      throw new RuntimeException("Lengths Iterator is done.");
    }
  }

  @Override
  public byte[] getRegionBytes() {
    if (head != null) {
      return ((LengthsReader.LengthsIterator) this.head).getRegionBytes();
    } else {
      throw new RuntimeException("Lengths Iterator is done.");
    }
  }
}
