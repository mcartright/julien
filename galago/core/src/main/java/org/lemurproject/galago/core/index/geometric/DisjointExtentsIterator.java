/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import java.util.Collection;
import org.lemurproject.galago.core.index.CountIterator;
import org.lemurproject.galago.core.index.ExtentIterator;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class DisjointExtentsIterator extends DisjointIndexesIterator implements ExtentIterator {

  public DisjointExtentsIterator(Collection<ExtentIterator> iterators) {
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
  public ExtentArray extents() {
    return ((ExtentIterator) head).extents();
  }

  @Override
  public ExtentArray getData() {
    return ((ExtentIterator) head).getData();
  }

  @Override
  public byte[] key() {
    return Utility.fromString("DisEI");
  }
}
