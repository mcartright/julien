// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.DataInput;
import java.io.IOException;
import org.lemurproject.galago.core.index.AggregateReader.AggregateIndexPart;
import org.lemurproject.galago.core.index.AggregateReader.IndexPartStatistics;
import org.lemurproject.galago.core.index.AggregateReader.NodeAggregateIterator;
import org.lemurproject.galago.core.index.AggregateReader.NodeStatistics;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.ExtentIterator;
import org.lemurproject.galago.core.index.Iterator;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.parse.stem.Stemmer;
import org.lemurproject.galago.core.util.ExtentArray;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.VByteInput;

/**
 * Reads a simple positions-based index, where each inverted list in the index
 * contains both term count information and term position information. The term
 * counts data is stored separately from term position information for faster
 * query processing when no positions are needed.
 *
 * (12/16/2010, irmarc): In order to facilitate faster count-only processing,
 * the default iterator created will not even open the positions list when
 * iterating. This is an interesting enough change that there are now two
 * versions of the iterator
 *
 * @author trevor, irmarc
 */
public class PositionIndexReader extends KeyListReader implements AggregateIndexPart {

  Stemmer stemmer = null;

  public PositionIndexReader(BTreeReader reader) throws Exception {
    super(reader);
    if (reader.getManifest().containsKey("stemmer")) {
      stemmer = (Stemmer) Class.forName(reader.getManifest().getString("stemmer")).newInstance();
    }
  }

  public PositionIndexReader(String pathname) throws Exception {
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
  public TermExtentIterator getTermExtents(byte[] term) throws IOException {
    BTreeReader.BTreeIterator iterator = reader.getIterator(term);
    if (iterator != null) {
      return new TermExtentIterator(iterator);
    }
    return null;
  }

  @Override
  public Iterator getIterator(byte[] key) throws IOException {
    return getTermExtents(key);
  }

  @Override
  public IndexPartStatistics getStatistics() {
    Parameters manifest = this.getManifest();
    IndexPartStatistics is = new IndexPartStatistics();
    is.collectionLength = manifest.get("statistics/collectionLength", 0);
    is.vocabCount = manifest.get("statistics/vocabCount", 0);
    is.highestDocumentCount = manifest.get("statistics/highestDocumentCount", 0);
    is.highestFrequency = manifest.get("statistics/highestFrequency", 0);
    is.partName = manifest.get("filename", "PositionIndexPart");
    return is;
  }

  // subclasses 
  public class KeyIterator extends KeyListReader.KeyValueIterator {

    public KeyIterator(BTreeReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getValueString() {
      TermExtentIterator it;
      long count = -1;
      try {
        it = new TermExtentIterator(iterator);
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
      return new TermExtentIterator(iterator);
    }

    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(getKey());
    }
  }

  public class TermExtentIterator extends KeyListReader.ListIterator
          implements NodeAggregateIterator, ExtentIterator {

    private BTreeReader.BTreeIterator iterator;
    private int documentCount;
    private int totalPositionCount;
    private int maximumPositionCount;
    private VByteInput documents;
    private VByteInput counts;
    private VByteInput positions;
    private int documentIndex;
    private int currentDocument;
    private int currentCount;
    private ExtentArray extentArray;
    private final ExtentArray emptyExtentArray;
    // to support resets
    protected long startPosition, endPosition;
    // to support skipping
    private VByteInput skips;
    private VByteInput skipPositions;
    private DataStream skipPositionsStream;
    private DataStream documentsStream;
    private DataStream countsStream;
    private DataStream positionsStream;
    private int skipDistance;
    private int skipResetDistance;
    private long numSkips;
    private long skipsRead;
    private long nextSkipDocument;
    private long lastSkipPosition;
    private long documentsByteFloor;
    private long countsByteFloor;
    private long positionsByteFloor;
    // Supports lazy-loading of extents
    private boolean extentsLoaded;
    private int inlineMinimum;
    private int extentsByteSize;

    public TermExtentIterator(BTreeReader.BTreeIterator iterator) throws IOException {
      super(iterator.getKey());
      extentArray = new ExtentArray();
      emptyExtentArray = new ExtentArray();
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

      if ((options & HAS_INLINING) == HAS_INLINING) {
        inlineMinimum = stream.readInt();
      } else {
        inlineMinimum = Integer.MAX_VALUE;
      }

      documentCount = stream.readInt();
      totalPositionCount = stream.readInt();

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
      long positionsByteLength = stream.readLong();
      long skipsByteLength = 0;
      long skipPositionsByteLength = 0;

      if ((options & HAS_SKIPS) == HAS_SKIPS) {
        skipsByteLength = stream.readLong();
        skipPositionsByteLength = stream.readLong();
      }

      long documentStart = valueStream.getPosition();
      long countsStart = documentStart + documentByteLength;
      long positionsStart = countsStart + countsByteLength;
      long positionsEnd = positionsStart + positionsByteLength;

      documentsStream = iterator.getSubValueStream(documentStart, documentByteLength);
      countsStream = iterator.getSubValueStream(countsStart, countsByteLength);
      positionsStream = iterator.getSubValueStream(positionsStart, positionsByteLength);

      documents = new VByteInput(documentsStream);
      counts = new VByteInput(countsStream);
      positions = new VByteInput(positionsStream);

      if ((options & HAS_SKIPS) == HAS_SKIPS) {

        long skipsStart = positionsStart + positionsByteLength;
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
        positionsByteFloor = 0;
      } else {
        assert positionsEnd == endPosition - startPosition;
        skips = null;
        skipPositions = null;
      }

      documentIndex = 0;
      extentsLoaded = true; // Not really, but this keeps it from reading ahead too soon.
      loadNextPosting();
    }

    private void loadNextPosting() throws IOException {
      if (!extentsLoaded) {
        if (currentCount > inlineMinimum) {
          positions.skipBytes(extentsByteSize);
        } else {
          loadExtents();
        }
      }
      currentDocument += documents.readInt();
      currentCount = counts.readInt();

      // Prep the extents
      extentArray.reset();
      extentsLoaded = false;
      if (currentCount > inlineMinimum) {
        extentsByteSize = positions.readInt();
      } else {
        // Load them aggressively since we can't skip them
        loadExtents();
      }
    }

    // Loads up a single set of positions for an intID. Basically it's the
    // load that needs to be done when moving forward one in the posting list.
    private void loadExtents() throws IOException {
      if (!extentsLoaded) {
        extentArray.setDocument(currentDocument);
        int position = 0;
        for (int i = 0; i < currentCount; i++) {
          position += positions.readInt();
          extentArray.add(position);
        }
        extentsLoaded = true;
      }
    }

    @Override
    public String getEntry() throws IOException {
      StringBuilder builder = new StringBuilder();

      builder.append(Utility.toString(key()));
      builder.append(",");
      builder.append(currentDocument);
      ExtentArray e = extents();
      for (int i = 0; i < e.size(); ++i) {
        builder.append(",");
        builder.append(e.begin(i));
      }

      return builder.toString();
    }

    @Override
    public void reset(BTreeReader.BTreeIterator i) throws IOException {
      iterator = i;
      key = iterator.getKey();
      startPosition = iterator.getValueStart();
      endPosition = iterator.getValueEnd();
      reset();
    }

    @Override
    public void reset() throws IOException {
      currentDocument = 0;
      currentCount = 0;
      extentArray.reset();
      initialize();
    }

    @Override
    public void movePast(int document) throws IOException {
      syncTo(document + 1);
    }

    // If we have skips - it's go time
    @Override
    public void syncTo(int document) throws IOException {
      if (skips != null) {
        synchronizeSkipPositions();
      }
      if (skips != null && document > nextSkipDocument) {
        extentsLoaded = true;
        extentsByteSize = 0;
        // if we're here, we're skipping
        while (skipsRead < numSkips
                && document > nextSkipDocument) {
          skipOnce();
        }
        repositionMainStreams();
      }

      // Linear from here
      while (!isDone() && document > currentDocument) {
        documentIndex = Math.min(documentIndex + 1, documentCount);
        if (!isDone()) {
          loadNextPosting();
        }
      }
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
        positionsByteFloor = skipPositions.readLong();
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
        positionsStream.seek(positionsByteFloor);
      } else {
        skipPositionsStream.seek(lastSkipPosition);
        documentsStream.seek(documentsByteFloor + skipPositions.readInt());
        countsStream.seek(countsByteFloor + skipPositions.readInt());
        positionsStream.seek(positionsByteFloor + skipPositions.readLong());
      }
      documentIndex = (int) (skipDistance * skipsRead) - 1;
    }

    @Override
    public boolean isDone() {
      return documentIndex >= documentCount;
    }

    @Override
    public ExtentArray getData() {
      try {
        loadExtents();
        return extentArray;
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    @Override
    public ExtentArray extents() {
      try {
        loadExtents();
        return extentArray;
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
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
      return ((long) documentCount);
    }

    @Override
    public NodeStatistics getStatistics() {
      NodeStatistics stats = new NodeStatistics();
      stats.node = Utility.toString(this.key);
      stats.nodeFrequency = this.totalPositionCount;
      stats.nodeDocumentCount = this.documentCount;
      stats.maximumCount = this.maximumPositionCount;
      return stats;
    }
  }
}