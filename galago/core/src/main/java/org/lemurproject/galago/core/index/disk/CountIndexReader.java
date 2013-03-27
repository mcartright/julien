// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.DataInput;
import java.io.IOException;
import org.lemurproject.galago.core.index.AggregateReader.AggregateIndexPart;
import org.lemurproject.galago.core.index.AggregateReader.IndexPartStatistics;
import org.lemurproject.galago.core.index.AggregateReader.NodeAggregateIterator;
import org.lemurproject.galago.core.index.AggregateReader.NodeStatistics;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.CountIterator;
import org.lemurproject.galago.core.index.Iterator;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.parse.stem.Stemmer;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.VByteInput;

/**
 * Reads a count based index structure mapping( term -> list(document-id),
 * list(document-freq) )
 *
 * Skip lists are supported
 *
 * @author sjh
 */
public class CountIndexReader extends KeyListReader implements AggregateIndexPart {

  Stemmer stemmer = null;

  public CountIndexReader(BTreeReader reader) throws Exception {
    super(reader);
    if (reader.getManifest().containsKey("stemmer")) {
      stemmer = (Stemmer) Class.forName(reader.getManifest().getString("stemmer")).newInstance();
    }
  }

  public CountIndexReader(String pathname) throws Exception {
    super(pathname);
    if (reader.getManifest().containsKey("stemmer")) {
      stemmer = (Stemmer) Class.forName(reader.getManifest().getString("stemmer")).newInstance();
    }
  }

  @Override
  public KeyIterator keys() throws IOException {
    return new KeyIterator(reader);
  }

  /**
   * Returns an iterator pointing at the specified term, or null if the term
   * doesn't exist in the inverted file.
   */
  public TermCountIterator getTermCounts(byte[] key) throws IOException {
    BTreeReader.BTreeIterator iterator = reader.getIterator(key);

    if (iterator != null) {
      return new TermCountIterator(iterator);
    }
    return null;
  }

  public TermCountIterator getTermCounts(String term) throws IOException {
    return getTermCounts(Utility.fromString(stemAsRequired(term)));
  }

  @Override
  public Iterator getIterator(byte[] key) throws IOException {
    return getTermCounts(key);
  }

  @Override
  public IndexPartStatistics getStatistics() {
    Parameters manifest = this.getManifest();
    IndexPartStatistics is = new IndexPartStatistics();
    is.collectionLength = manifest.get("statistics/collectionLength", 0);
    is.vocabCount = manifest.get("statistics/vocabCount", 0);
    is.highestDocumentCount = manifest.get("statistics/highestDocumentCount", 0);
    is.highestFrequency = manifest.get("statistics/highestFrequency", 0);
    is.partName = manifest.get("filename", "CountIndexPart");
    return is;
  }

  private String stemAsRequired(String term) {
    if (stemmer != null) {
      if (term.contains("~")) {
        return stemmer.stemWindow(term);
      } else {
        return stemmer.stem(term);
      }
    }
    return term;
  }

  public class KeyIterator extends KeyListReader.KeyValueIterator {

    public KeyIterator(BTreeReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getValueString() {
      TermCountIterator it;
      long count = -1;
      try {
        it = new TermCountIterator(iterator);
        count = it.count();
      } catch (IOException ioe) {
      }
      StringBuilder sb = new StringBuilder();
      sb.append(Utility.toString(getKey())).append(",");
      sb.append("list of size: ");
      if (count > 0) {
        sb.append(count);
      } else {
        sb.append("Unknown");
      }
      return sb.toString();
    }

    @Override
    public Iterator getValueIterator() throws IOException {
      return new TermCountIterator(iterator);
    }

    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(iterator.getKey());
    }
  }

  public class TermCountIterator extends KeyListReader.ListIterator
          implements NodeAggregateIterator, CountIterator {

    BTreeReader.BTreeIterator iterator;
    int documentCount;
    int collectionCount;
    int maximumPositionCount;
    VByteInput documents;
    VByteInput counts;
    int documentIndex;
    int currentDocument;
    int currentCount;
    // Support for resets
    long startPosition, endPosition;
    // to support skipping
    VByteInput skips;
    VByteInput skipPositions;
    DataStream skipPositionsStream;
    DataStream documentsStream;
    DataStream countsStream;
    int skipDistance;
    int skipResetDistance;
    long numSkips;
    long skipsRead;
    long nextSkipDocument;
    long lastSkipPosition;
    long documentsByteFloor;
    long countsByteFloor;

    public TermCountIterator(BTreeReader.BTreeIterator iterator) throws IOException {
      super(iterator.getKey());
      reset(iterator);
    }

    // Initialization method.
    //
    // Even though we check for skips multiple times, in terms of how the data is loaded
    // its easier to do the parts when appropriate
    protected void initialize() throws IOException {
      DataStream valueStream = iterator.getSubValueStream(0, iterator.getValueLength());
      DataInput stream = new VByteInput(valueStream);

      // metadata
      int options = stream.readInt();
      documentCount = stream.readInt();
      collectionCount = stream.readInt();

      if ((options & HAS_MAXTF) == HAS_MAXTF) {
        maximumPositionCount = stream.readInt();
      } else {
        maximumPositionCount = Integer.MAX_VALUE;
      }

      if ((options & HAS_SKIPS) == HAS_SKIPS) {
        skipDistance = stream.readInt();
        skipResetDistance = stream.readInt();
        numSkips = stream.readLong();
      }

      // segment lengths
      long documentByteLength = stream.readLong();
      long countsByteLength = stream.readLong();
      long skipsByteLength = 0;
      long skipPositionsByteLength = 0;

      if ((options & HAS_SKIPS) == HAS_SKIPS) {
        skipsByteLength = stream.readLong();
        skipPositionsByteLength = stream.readLong();
      }

      long documentStart = valueStream.getPosition();
      long countsStart = documentStart + documentByteLength;
      long countsEnd = countsStart + countsByteLength;

      documentsStream = iterator.getSubValueStream(documentStart, documentByteLength);
      countsStream = iterator.getSubValueStream(countsStart, countsByteLength);

      documents = new VByteInput(documentsStream);
      counts = new VByteInput(countsStream);

      if ((options & HAS_SKIPS) == HAS_SKIPS) {

        long skipsStart = countsEnd;
        long skipPositionsStart = skipsStart + skipsByteLength;
        long skipPositionsEnd = skipPositionsStart + skipPositionsByteLength;

        assert skipPositionsEnd == endPosition - startPosition;

        skips = new VByteInput(iterator.getSubValueStream(skipsStart, skipsByteLength));
        skipPositionsStream = iterator.getSubValueStream(skipPositionsStart, skipPositionsByteLength);
        skipPositions = new VByteInput(skipPositionsStream);

        // load up
        nextSkipDocument = skips.readInt();
        documentsByteFloor = 0;
        countsByteFloor = 0;
      } else {
        assert countsEnd == endPosition - startPosition;
        skips = null;
        skipPositions = null;
      }

      documentIndex = 0;
      load();
    }

    // Only loading the docid and the count
    private void load() throws IOException {
      currentDocument += documents.readInt();
      currentCount = counts.readInt();
    }

    @Override
    public String getEntry() throws IOException {
      StringBuilder builder = new StringBuilder();

      builder.append(Utility.toString(key()));
      builder.append(",");
      builder.append(currentDocument);
      builder.append(",");
      builder.append(currentCount);

      return builder.toString();
    }

    @Override
    public void reset(BTreeReader.BTreeIterator i) throws IOException {
      iterator = i;
      startPosition = iterator.getValueStart();
      endPosition = iterator.getValueEnd();
      key = iterator.getKey();
      initialize();
    }

    @Override
    public void reset() throws IOException {
      currentDocument = 0;
      currentCount = 0;
      initialize();
    }

    // If we have skips - it's go time
    @Override
    public void syncTo(int document) throws IOException {
      if (skips != null) {
        synchronizeSkipPositions();
        if (document > nextSkipDocument) {
          // if we're here, we're skipping
          while (skipsRead < numSkips
                  && document > nextSkipDocument) {
            skipOnce();
          }
          repositionMainStreams();
        }
      }


      // linear from here
      while (!isDone() && document > currentDocument) {
        documentIndex = Math.min(documentIndex + 1, documentCount);
        if (!isDone()) {
          load();
        }
      }
    }

    @Override
    public void movePast(int document) throws IOException {
      syncTo(document + 1);
    }

    // This only moves forward in tier 1, reads from tier 2 only when
    // needed to update floors
    //
    private void skipOnce() throws IOException {
      assert skipsRead < numSkips;
      long currentSkipPosition = lastSkipPosition + skips.readInt();

      if (skipsRead % skipResetDistance == 0) {
        // Position the skip positions stream
        skipPositionsStream.seek(currentSkipPosition);

        // now set the floor values
        documentsByteFloor = skipPositions.readInt();
        countsByteFloor = skipPositions.readInt();
      }
      currentDocument = (int) nextSkipDocument;

      // May be at the end of the buffer
      if (skipsRead + 1 == numSkips) {
        nextSkipDocument = Integer.MAX_VALUE;
      } else {
        nextSkipDocument += skips.readInt();
      }
      skipsRead++;
      lastSkipPosition = currentSkipPosition;
    }

    // This makes sure the skip list pointers are still ahead of the current document.
    // If we called "next" a lot, these may be out of sync.
    //
    private void synchronizeSkipPositions() throws IOException {
      while (nextSkipDocument <= currentDocument) {
        int cd = currentDocument;
        skipOnce();
        currentDocument = cd;
      }
    }

    private void repositionMainStreams() throws IOException {
      // If we just reset the floors, don't read the 2nd tier again
      if ((skipsRead - 1) % skipResetDistance == 0) {
        documentsStream.seek(documentsByteFloor);
        countsStream.seek(countsByteFloor);
      } else {
        skipPositionsStream.seek(lastSkipPosition);
        documentsStream.seek(documentsByteFloor + skipPositions.readInt());
        countsStream.seek(countsByteFloor + skipPositions.readInt());
        // we seek here, so no reading needed
      }
      documentIndex = (int) (skipDistance * skipsRead) - 1;
    }

    @Override
    public boolean isDone() {
      return documentIndex >= documentCount;
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
    public int count() {
      return currentCount;
    }

    @Override
    public int maximumCount() {
      return maximumPositionCount;
    }

    @Override
    public long totalEntries() {
      return documentCount;
    }

    @Override
    public NodeStatistics getStatistics() {
      NodeStatistics stats = new NodeStatistics();
      stats.node = Utility.toString(this.key);
      stats.nodeFrequency = this.collectionCount;
      stats.nodeDocumentCount = this.documentCount;
      stats.maximumCount = this.maximumPositionCount;
      return stats;
    }
  }
}