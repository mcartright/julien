/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import java.util.Collection;
import org.lemurproject.galago.core.index.CountIterator;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class DisjointCountsIterator extends DisjointIndexesIterator implements CountIterator {

  public DisjointCountsIterator(Collection<CountIterator> iterators) {
    super((Collection) iterators);
  }

  @Override
  public int count() {
    return ((CountIterator) head).count();
  }

  @Override
  public int maximumCount() {
    return ((CountIterator) head).maximumCount();
  }

  @Override
  public byte[] key() {
    return Utility.fromString("DisCI");
  }
}
