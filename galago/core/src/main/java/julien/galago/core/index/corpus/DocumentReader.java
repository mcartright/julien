// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index.corpus;

import java.io.IOException;

import julien.galago.core.index.Index;
import julien.galago.core.index.KeyIterator;
import julien.galago.core.parse.Document;
import julien.galago.tupleflow.Parameters;


/**
 * Interface that allows different corpus formats
 * See CorpusReader / DocumentIndexReader / MemoryCorpus
 *
 * @author sjh
 */
public interface DocumentReader extends Index.IndexPartReader {

  public abstract Document getDocument(byte[] key, Parameters p) throws IOException;

  public abstract Document getDocument(int key, Parameters p) throws IOException;

  public interface DocumentIterator extends KeyIterator {

    public abstract Document getDocument(Parameters p) throws IOException;
  }
}
