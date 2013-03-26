// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;
import org.lemurproject.galago.core.index.Index.IndexComponentReader;

/**
 * A StructuredIndexPart is an object that can create StructuredIterators that
 * can be used in query processing.  StructuredIndex creates many StructuredIndexPartReaders
 * and uses them to supply iterators to StructuredRetrieval.
 * 
 * Usually a IndexPartReader uses an IndexReader to retrieve data from disk,
 * then adds its own special logic to decode that data.
 * 
 * @author trevor
 */
public interface IndexPartReader extends IndexComponentReader {

  /// Returns an iterator over the keys of the index.
  public KeyIterator keys() throws IOException;

  /// Returns an iterator corresponding to a query node from a StructuredQuery.
  /// The type of iterator returned is assumed to be a value iterator (i.e. over one
  /// list in the index)
  public Iterator getIterator(byte[] key) throws IOException;
}
