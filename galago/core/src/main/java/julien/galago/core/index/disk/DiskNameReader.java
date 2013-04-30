// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index.disk;

import java.io.IOException;

import julien.galago.core.index.BTreeFactory;
import julien.galago.core.index.BTreeReader;
import julien.galago.core.index.DataIterator;
import julien.galago.core.index.KeyToListIterator;
import julien.galago.core.index.KeyValueReader;
import julien.galago.core.index.NamesReader;
import julien.galago.tupleflow.Utility;


/**
 * Reads a binary file of document names produced by DocumentNameWriter2
 *
 * @author sjh
 */
public class DiskNameReader extends KeyValueReader implements NamesReader {

  /**
   * Creates a new instance of DiskNameReader
   */
  public DiskNameReader(String fileName) throws IOException {
    super(BTreeFactory.getBTreeReader(fileName));
  }

  public DiskNameReader(BTreeReader r) {
    super(r);
  }

  // gets the document name of the internal id index.
  @Override
  public String getDocumentName(int index) throws IOException {
    byte[] data = reader.getValueBytes(Utility.fromInt(index));
    if (data == null) {
      throw new IOException("Unknown Document Number : " + index);
    }
    return Utility.toString(data);
  }

  // gets the document id for some document name
  @Override
  public int getDocumentIdentifier(String documentName) throws IOException {
    throw new UnsupportedOperationException("This index file does not support doc name -> doc int mappings");
  }

  @Override
  public NamesReader.NamesIterator getNamesIterator() throws IOException {
    return new ValueIterator(keys());
  }

  @Override
  public KeyIterator keys() throws IOException {
    return new KeyIterator(reader);
  }

  @Override
  public KeyToListIterator getIterator(byte[] keys) throws IOException {
    return (KeyToListIterator) getNamesIterator();
  }

  public class KeyIterator extends KeyValueReader.KeyValueIterator {

    protected BTreeReader input;
    protected BTreeReader.BTreeIterator iterator;

    public KeyIterator(BTreeReader input) throws IOException {
      super(input);
    }

    public boolean skipToKey(int identifier) throws IOException {
      return skipToKey(Utility.fromInt(identifier));
    }

    public String getCurrentName() throws IOException {
      return Utility.toString(getValueBytes());
    }

    public int getCurrentIdentifier() throws IOException {
      return Utility.toInt(getKey());
    }

    @Override
    public String getValueString() {
      try {
        return Utility.toString(getValueBytes());
      } catch (IOException e) {
        return "Unknown";
      }
    }

    @Override
    public String getKeyString() {
      return Integer.toString(Utility.toInt(getKey()));
    }

    @Override
    public KeyToListIterator getValueIterator() throws IOException {
      return new ValueIterator(this);
    }
  }

  public class ValueIterator extends KeyToListIterator implements DataIterator<String>, NamesReader.NamesIterator {

    public ValueIterator(KeyIterator ki) {
      super(ki);
    }

    @Override
    public String getEntry() throws IOException {
      KeyIterator ki = (KeyIterator) iterator;
      StringBuilder sb = new StringBuilder();
      sb.append(ki.getCurrentIdentifier());
      sb.append(",");
      sb.append(ki.getCurrentName());
      return sb.toString();
    }

    @Override
    public long totalEntries() {
      return reader.getManifest().getLong("keyCount");
    }

    @Override
    public boolean hasAllCandidates() {
      return true;
    }

    @Override
    public String getData() {
      try {
        return getCurrentName();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    @Override
    public String getCurrentName() throws IOException {
      KeyIterator ki = (KeyIterator) iterator;
      return ki.getCurrentName();
    }

    @Override
    public int getCurrentIdentifier() throws IOException {
      KeyIterator ki = (KeyIterator) iterator;
      return ki.getCurrentIdentifier();
    }

    @Override
    public byte[] key() {
      return Utility.fromString("names");
    }
  }
}
