// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import julien.galago.core.index.BTreeReader;
import julien.galago.core.index.IndicatorIterator;
import julien.galago.core.index.KeyToListIterator;
import julien.galago.core.index.KeyValueReader;
import julien.galago.tupleflow.Parameters;
import julien.galago.tupleflow.Utility;


/**
 * 
 * @author sjh
 * @author irmarc
 */
public class DocumentIndicatorReader extends KeyValueReader {

  protected boolean def;
  protected Parameters manifest;

  public DocumentIndicatorReader(String filename) throws FileNotFoundException, IOException {
    super(filename);
    this.manifest = this.reader.getManifest();
    def = this.manifest.get("default", false);  // Play conservative
  }

  public DocumentIndicatorReader(BTreeReader r) {
    super(r);
  }

  public boolean getIndicator(int document) throws IOException {
    byte[] valueBytes = reader.getValueBytes(Utility.fromInt(document));
    if ((valueBytes == null) || (valueBytes.length == 0)) {
      return def;
    } else {
      return Utility.toBoolean(valueBytes);
    }
  }

  @Override
  public KeyIterator keys() throws IOException {
    return new KeyIterator(reader);
  }

  @Override
  public ValueIterator getIterator(byte[] key) throws IOException {
    return new ValueIterator(new KeyIterator(reader));
  }

  public class KeyIterator extends KeyValueReader.KeyValueIterator {

    public KeyIterator(BTreeReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getKeyString() {
      return Integer.toString(getCurrentDocument());
    }

    @Override
    public String getValueString() {
      try {
        return Boolean.toString(getCurrentIndicator());
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    public boolean skipToKey(int key) throws IOException {
      return skipToKey(Utility.fromInt(key));
    }

    public int getCurrentDocument() {
      return Utility.toInt(iterator.getKey());
    }

    public boolean getCurrentIndicator() throws IOException {
      byte[] valueBytes = iterator.getValueBytes();
      if ((valueBytes == null) || (valueBytes.length == 0)) {
        return def;
      } else {
        return Utility.toBoolean(valueBytes);
      }
    }

    @Override
    public boolean isDone() {
      return iterator.isDone();
    }

    @Override
    public ValueIterator getValueIterator() throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  // needs to be an AbstractIndicator
  public class ValueIterator extends KeyToListIterator implements IndicatorIterator {

    boolean defInst;

    public ValueIterator(KeyIterator it) {
      super(it);
      this.defInst = def; // same as indri
    }

    @Override
    public String getEntry() throws IOException {
      return Integer.toString(((KeyIterator) iterator).getCurrentDocument());
    }

    @Override
    public long totalEntries() {
      return manifest.get("keyCount", -1);
    }

    @Override
    public boolean hasAllCandidates() {
      return false;
    }

    @Override
    public boolean hasMatch(int document) {
      return super.hasMatch(document) && indicator(document);
    }

    @Override
    public boolean indicator(int document) {
      if (document == currentCandidate()) {
        try {
          return ((KeyIterator) iterator).getCurrentIndicator();
        } catch (IOException ex) {
          Logger.getLogger(DocumentIndicatorReader.class.getName()).log(Level.SEVERE, null, ex);
          throw new RuntimeException("Failed to read indicator file.");
        }
      }
      return this.defInst;
    }

    @Override
    public byte[] key() {
      return Utility.fromString("indicators");
    }
  }
}
