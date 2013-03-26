// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.Iterator;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.index.ScoreIterator;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.VByteInput;

/**
 * Retrieves lists of floating point numbers which can be used as document
 * features.
 *
 * @author trevor
 */
public class SparseFloatListReader extends KeyListReader {

  private static double defaultScore = Math.log(Math.pow(10, -10));

  public SparseFloatListReader(String pathname) throws FileNotFoundException, IOException {
    super(pathname);
  }

  @Override
  public KeyIterator keys() throws IOException {
    return new KeyIterator(reader);
  }

  public ListIterator getListIterator() throws IOException {
    return new ListIterator(reader.getIterator(), defaultScore);
  }

  @Override
  public Iterator getIterator(byte[] key) throws IOException {
    BTreeReader.BTreeIterator iterator = reader.getIterator(key);
    return new ListIterator(iterator, defaultScore);
  }

  public class KeyIterator extends KeyListReader.KeyValueIterator {

    public KeyIterator(BTreeReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getValueString() {
      ListIterator it;
      long count = -1;
      try {
        it = new ListIterator(iterator, defaultScore);
        count = it.totalEntries();
      } catch (IOException ioe) {
      }

      StringBuilder sb = new StringBuilder();
      sb.append(Utility.toString(iterator.getKey())).append(", List Value: size=");
      if (count > 0) {
        sb.append(count);
      } else {
        sb.append("Unknown");
      }
      return sb.toString();
    }

    @Override
    public ListIterator getValueIterator() throws IOException {
      return new ListIterator(iterator, defaultScore);
    }

    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(iterator.getKey());
    }
  }

  public class ListIterator extends KeyListReader.ListIterator
          implements ScoreIterator {

    VByteInput stream;
    int documentCount;
    int index;
    int currentDocument;
    double currentScore;
    double def;

    public ListIterator(BTreeReader.BTreeIterator iterator, double defaultScore) throws IOException {
      super(iterator.getKey());
      reset(iterator);
      def = defaultScore;
    }

    void read() throws IOException {
      index += 1;

      if (index < documentCount) {
        currentDocument += stream.readInt();
        currentScore = stream.readFloat();
      } else {
        // ensure we never overflow
        index = documentCount;
      }
    }

    @Override
    public String getEntry() throws IOException {
      StringBuilder builder = new StringBuilder();

      builder.append(Utility.toString(key()));
      builder.append(",");
      builder.append(currentDocument);
      builder.append(",");
      builder.append(currentScore);

      return builder.toString();
    }

    @Override
    public void reset(BTreeReader.BTreeIterator iterator) throws IOException {
      DataStream buffered = iterator.getValueStream();
      stream = new VByteInput(buffered);
      documentCount = stream.readInt();
      index = -1;
      currentDocument = 0;
      if (documentCount > 0) {
        read();
      }
    }

    @Override
    public void reset() throws IOException {
      throw new UnsupportedOperationException("This iterator does not reset without the parent KeyIterator.");
    }

    @Override
    public int currentCandidate() {
      return currentDocument;
    }

    @Override
    public boolean hasAllCandidates() {
      return false;
    }

    @Override
    public void movePast(int document) throws IOException {
      while (!isDone() && document >= currentDocument) {
        read();
      }
    }

    @Override
    public void syncTo(int document) throws IOException {
      while (!isDone() && document > currentDocument) {
        read();
      }
    }

    @Override
    public double score() {
      return currentScore;
    }

    @Override
    public boolean isDone() {
      return index >= documentCount;
    }

    @Override
    public long totalEntries() {
      return documentCount;
    }

    @Override
    public double maximumScore() {
      return Double.POSITIVE_INFINITY;
    }

    @Override
    public double minimumScore() {
      return Double.NEGATIVE_INFINITY;
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
