// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index.disk;

import java.io.IOException;

import julien.galago.core.index.BTreeFactory;
import julien.galago.core.index.BTreeReader;
import julien.galago.core.index.KeyToListIterator;
import julien.galago.core.index.KeyValueReader;
import julien.galago.core.index.NamesReader;
import julien.galago.tupleflow.Utility;


/**
 * Reads a binary file of document names produced by DocumentNameWriter2
 * 
 * @author sjh
 */
public class DiskNameReverseReader extends KeyValueReader implements NamesReader {

  /** Creates a new instance of DiskNameReader */
  public DiskNameReverseReader(String fileName) throws IOException {
    super(BTreeFactory.getBTreeReader(fileName));
  }

  public DiskNameReverseReader(BTreeReader r) {
    super(r);
  }

  // gets the document name of the internal id index.
  @Override
  public String getDocumentName(int index) throws IOException {
    throw new UnsupportedOperationException("This index file does not support doc int -> doc name mappings");
  }

  // gets the document id for some document name
  @Override
  public int getDocumentIdentifier(String documentName) throws IOException {
    byte[] data = reader.getValueBytes(Utility.fromString(documentName));
    if (data == null) {
      throw new IOException("Unknown Document Name : " + documentName);
    }
    return Utility.toInt(data);
  }

  @Override
  public NamesReader.NamesIterator getNamesIterator() throws IOException {
    throw new UnsupportedOperationException("This index file does not support doc int -> doc name mappings");
  }

  @Override
  public KeyIterator keys() throws IOException {
    return new KeyIterator(reader);
  }

  @Override
  public KeyToListIterator getIterator(byte[] key) throws IOException {
    throw new UnsupportedOperationException(
            "Index doesn't support iteration");
  }

  public class KeyIterator extends KeyValueReader.KeyValueIterator {

    protected BTreeReader input;

    public KeyIterator(BTreeReader input) throws IOException {
      super(input);
    }

    public boolean skipToKey(String name) throws IOException {
      return findKey(Utility.fromString(name));
    }

    public String getCurrentName() throws IOException {
      return Utility.toString(getKey());
    }

    public int getCurrentIdentifier() throws IOException {
      return Utility.toInt(getValueBytes());
    }

    @Override
    public String getValueString() {
      try {
        return Integer.toString(Utility.toInt(getValueBytes()));
      } catch (IOException e) {
        return "Unknown";
      }
    }

    @Override
    public String getKeyString() {
      return Utility.toString(getKey());
    }

    @Override
    public KeyToListIterator getValueIterator() throws IOException {
      throw new UnsupportedOperationException("This index file does not support doc int -> doc name mappings");
    }
  }
}
