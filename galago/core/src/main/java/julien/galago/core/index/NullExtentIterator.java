// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index;

import java.io.IOException;

import julien.galago.core.index.AggregateReader.NodeAggregateIterator;
import julien.galago.core.index.AggregateReader.NodeStatistics;
import julien.galago.core.util.ExtentArray;
import julien.galago.tupleflow.Utility;


/**
 *
 * @author trevor
 * @author irmarc
 */
public class NullExtentIterator
        implements ExtentIterator, NodeAggregateIterator {

  ExtentArray array = new ExtentArray();
  byte[] key;
  
  public NullExtentIterator(byte[] key) {
    this.key = key;
  }

  @Override
  public byte[] key() {
    return key;
  }

  public boolean nextEntry() {
    return false;
  }

  @Override
  public boolean isDone() {
    return true;
  }

  @Override
  public ExtentArray extents() {
    return array;
  }

  @Override
  public int count() {
    return 0;
  }

  @Override
  public int maximumCount() {
    return 0;
  }

  @Override
  public void reset() {
    // do nothing
  }

  @Override
  public long totalEntries() {
    return 0;
  }

  @Override
  public int currentCandidate() {
    return Integer.MAX_VALUE;
  }

  @Override
  public boolean hasMatch(int id) {
    return false;
  }

  @Override
  public String getEntry() throws IOException {
    return "NULL";
  }

  @Override
  public boolean syncTo(int identifier) throws IOException {
    return false;
  }

  @Override
  public int movePast(int identifier) throws IOException {
    return Integer.MAX_VALUE;
  }

  @Override
  public int compareTo(Iterator t) {
    return 1;
  }

  @Override
  public boolean hasAllCandidates() {
    return false;
  }

  @Override
  public NodeStatistics getStatistics() {
    NodeStatistics stats = new NodeStatistics();
    stats.node = Utility.toString(this.key);
    stats.nodeFrequency = 0;
    stats.nodeDocumentCount = 0;
    stats.maximumCount = 0;
    return stats;
  }

  @Override
  public long sizeInBytes() throws IOException {
    throw new UnsupportedOperationException("Not supported yet.");
  }
}
