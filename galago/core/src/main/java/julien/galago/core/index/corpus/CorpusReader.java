// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index.corpus;

import java.io.FileNotFoundException;
import java.io.IOException;
import javax.xml.soap.Node;

import julien.galago.core.index.BTreeReader;
import julien.galago.core.index.DataIterator;
import julien.galago.core.index.Iterator;
import julien.galago.core.index.KeyToListIterator;
import julien.galago.core.index.KeyValueReader;
import julien.galago.core.parse.Document;
import julien.galago.core.parse.PseudoDocument;
import julien.galago.tupleflow.Parameters;
import julien.galago.tupleflow.Utility;


/**
 *
 * Reader for corpus folders - corpus folder is a parallel index structure: -
 * one key.index file - several data files (0 -> n)
 *
 *
 * @author sjh
 */
public class CorpusReader extends KeyValueReader implements DocumentReader {

  public CorpusReader(String fileName) throws FileNotFoundException, IOException {
    super(fileName);
  }

  public CorpusReader(BTreeReader r) {
    super(r);
  }

  @Override
  public KeyIterator keys() throws IOException {
    return new KeyIterator(reader);
  }

  @Override
  public Document getDocument(byte[] key, Parameters p) throws IOException {
    KeyIterator i = new KeyIterator(reader);
    if (i.findKey(key)) {
      return i.getDocument(p);
    } else {
      return null;
    }
  }

  @Override
  public Document getDocument(int key, Parameters p) throws IOException {
    KeyIterator i = new KeyIterator(reader);
    byte[] k = Utility.fromInt(key);
    if (i.findKey(k)) {
      return i.getDocument(p);
    } else {
      return null;
    }
  }

  @Override
  public Iterator getIterator(byte[] key) throws IOException {
    return new CorpusIterator(new KeyIterator(reader));
  }

  public class KeyIterator extends KeyValueReader.KeyValueIterator implements DocumentIterator {

    public KeyIterator(BTreeReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getKeyString() {
      return Integer.toString(Utility.toInt(getKey()));
    }

    @Override
    public Document getDocument(Parameters p) throws IOException {
      if (p.get("pseudo", false)) {
        return PseudoDocument.deserialize(iterator.getValueBytes(), p);
      } else {
        return Document.deserialize(iterator.getValueBytes(), p);
      }
    }

    @Override
    public String getValueString() throws IOException {
      try {
        Parameters p = new Parameters();
        p.set("terms", false);
        p.set("tags", false);
        return getDocument(p).toString();
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public Iterator getValueIterator() throws IOException {
      return new CorpusIterator(this);
    }
  }

  public class CorpusIterator extends KeyToListIterator implements DataIterator<Document> {

    Parameters docParams;

    public CorpusIterator(KeyIterator ki) {
      super(ki);
      docParams = new Parameters();
    }

    @Override
    public String getEntry() throws IOException {
      return ((KeyIterator) iterator).getValueString();
    }

    @Override
    public long totalEntries() {
      return reader.getManifest().getLong("keyCount");
    }

    @Override
    public Document getData() {
      try {
      return ((KeyIterator) iterator).getDocument(docParams);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    @Override
    public boolean hasAllCandidates() {
      return true;
    }

    @Override
    public byte[] key() {
      return Utility.fromString("corpus");
    }
  }
}
