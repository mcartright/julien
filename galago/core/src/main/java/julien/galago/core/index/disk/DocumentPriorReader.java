// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index.disk;

import java.io.FileNotFoundException;
import java.io.IOException;

import julien.galago.core.index.BTreeReader;
import julien.galago.core.index.KeyToListIterator;
import julien.galago.core.index.KeyValueReader;
import julien.galago.core.index.ScoreIterator;
import julien.galago.tupleflow.Parameters;
import julien.galago.tupleflow.Utility;


/**
 *
 * @author sjh
 */
public class DocumentPriorReader extends KeyValueReader {

  private double def;
  protected Parameters manifest;

  public DocumentPriorReader(String filename)
      throws FileNotFoundException, IOException {
    super(filename);
    this.manifest = this.reader.getManifest();
    def =  this
	.getManifest()
	.get("default", Math.log(0.0000000001)); // this must exist
  }

  public DocumentPriorReader(BTreeReader r) {
    super(r);
    this.manifest = this.reader.getManifest();
    def = this
	.getManifest()
	.get("default", Math.log(0.0000000001)); // this must exist
  }

  public double getPrior(int document) throws IOException {
    byte[] valueBytes = reader.getValueBytes(Utility.fromInt(document));
    if ((valueBytes == null) || (valueBytes.length == 0)) {
      return def;
    } else {
      return Utility.toDouble(valueBytes);
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
        return Double.toString(getCurrentScore());
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

    public double getCurrentScore() throws IOException {
      byte[] valueBytes = iterator.getValueBytes();
      if ((valueBytes == null) || (valueBytes.length == 0)) {
        return def;
      } else {
        return Utility.toDouble(valueBytes);
      }
    }

    @Override
    public ValueIterator getValueIterator() throws IOException {
      return new ValueIterator(new KeyIterator(reader));
    }
  }

  public class ValueIterator
      extends KeyToListIterator
      implements ScoreIterator {

    double minScore;

    public ValueIterator(KeyIterator it) {
      super(it);
      this.minScore = def; // same as indri
    }

    @Override
    public String getEntry() throws IOException {
      return ((KeyIterator) iterator).getValueString();
    }

    @Override
    public long totalEntries() {
      return manifest.get("keyCount", -1);
    }

    @Override
    public boolean hasAllCandidates() {
      return true;
    }

    @Override
    public boolean hasMatch(int identifier) {
      return true;
    }

    @Override
    public double score() {
      try {
        byte[] valueBytes = iterator.getValueBytes();
        if ((valueBytes == null) || (valueBytes.length == 0)) {
          return minScore;
        } else {
          return Utility.toDouble(valueBytes);
        }
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    @Override
    public double maximumScore() {
      return manifest.get("maxScore", Double.POSITIVE_INFINITY);
    }

    @Override
    public double minimumScore() {
      return manifest.get("minScore", Double.NEGATIVE_INFINITY);
    }

    @Override
    public byte[] key() {
      return Utility.fromString("priors");
    }

    @Override
    public void setMaximumScore(double newMax) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setMinimumScore(double newMin) {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }
}
