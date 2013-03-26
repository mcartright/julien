// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.store;

import java.io.IOException;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Interface that allows different corpus formats
 * See CorpusReader / DocumentIndexReader / MemoryCorpus
 *
 * @author sjh
 */
public interface RetrievalItemReader<T> extends IndexPartReader {

  public abstract T get(byte[] key, Parameters p) throws IOException;

  public interface RetrievalItemIterator<T> extends KeyIterator {

    public abstract T get(Parameters p) throws IOException;
  }
}
