// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index.disk;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import julien.galago.core.index.BTreeReader;
import julien.galago.core.index.DataIterator;
import julien.galago.core.index.ExtentIterator;
import julien.galago.core.index.Iterator;
import julien.galago.core.index.KeyListReader;
import julien.galago.core.index.AggregateReader.NodeAggregateIterator;
import julien.galago.core.index.AggregateReader.NodeStatistics;
import julien.galago.core.util.ExtentArray;
import julien.galago.tupleflow.DataStream;
import julien.galago.tupleflow.Parameters;
import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.VByteInput;


/**
 * Provides a column-oriented view of document fields.
 *
 * @author irmarc
 */
public class PositionContentReader extends KeyListReader {

  public PositionContentReader(BTreeReader reader) throws Exception {
    super(reader);
  }

  public PositionContentReader(String pathname) throws Exception {
    super(pathname);
  }

  @Override
  public KeyIterator keys() throws IOException {
    return new KeyIterator(reader);
  }

  /**
   * Returns an iterator pointing at the specified term, or null if the term
   * doesn't exist in the inverted file.
   */
  public FieldContentIterator getFieldContent(byte[] term) throws IOException {
    BTreeReader.BTreeIterator iterator = reader.getIterator(term);
    if (iterator != null) {
      return new FieldContentIterator(iterator);
    }
    return null;
  }

  @Override
  public Iterator getIterator(byte[] key) throws IOException {
    return getFieldContent(key);
  }

  // subclasses
  public class KeyIterator extends KeyListReader.KeyValueIterator {

    public KeyIterator(BTreeReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getValueString() {
      FieldContentIterator it;
      long count = -1;
      try {
        it = new FieldContentIterator(iterator);
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
      return new FieldContentIterator(iterator);
    }

    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(getKey());
    }
  }

  public class FieldContentIterator extends KeyListReader.ListIterator
      implements ExtentIterator, DataIterator<List<List<String>>>,
		 NodeAggregateIterator {

    private BTreeReader.BTreeIterator iterator;
    private int documentCount;
    private int totalPositionCount;
    private int maximumPositionCount;
    private VByteInput documents;
    private VByteInput positions;
    private VByteInput entries;
    private int documentIndex;
    private int currentDocument;
    private int currentCount;
    private ExtentArray extentArray;
    private ArrayList<List<String>> entryList;
    // to support resets
    protected long startPosition, endPosition;
    // Supports lazy-loading of positions and content
    private boolean positionsLoaded;
    private boolean entriesLoaded;
    private int inlineMinimum;
    private int positionsByteSize;
    private int entriesByteSize;

    public FieldContentIterator(BTreeReader.BTreeIterator iterator)
	throws IOException {
      super(iterator.getKey());
      extentArray = new ExtentArray();
      entryList = new ArrayList<List<String>>();
      reset(iterator);
    }

    // Initialization method.
    protected void initialize() throws IOException {
      DataStream valueStream =
	  iterator.getSubValueStream(0, iterator.getValueLength());
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

      // segment lengths
      long documentByteLength = stream.readLong();
      long positionsByteLength = stream.readLong();
      long entriesByteLength = stream.readLong();

      long documentStart = valueStream.getPosition();
      long positionsStart = documentStart + documentByteLength;
      long positionsEnd = positionsStart + positionsByteLength;
      long entriesStart = positionsEnd;
      long entriesEnd = entriesStart + entriesByteLength;

      // sanity check
      assert entriesEnd == endPosition - startPosition;

      DataStream documentsStream =
	  iterator.getSubValueStream(documentStart, documentByteLength);
      DataStream positionsStream =
	  iterator.getSubValueStream(positionsStart, positionsByteLength);
      DataStream entriesStream =
	  iterator.getSubValueStream(entriesStart, entriesByteLength);

      documents = new VByteInput(documentsStream);
      positions = new VByteInput(positionsStream);
      entries = new VByteInput(entriesStream);

      documentIndex = 0;
      // Not really, but this keeps it from reading ahead too soon.
      positionsLoaded = true;
      entriesLoaded = true;
      loadNextPosting();
    }

    private void loadNextPosting() throws IOException {
      if (!positionsLoaded) {
        if (currentCount > inlineMinimum) {
          positions.skipBytes(positionsByteSize);
        } else {
          loadPositions();
        }
      }
      if (!entriesLoaded) {
	  if (currentCount > inlineMinimum) {
	      entries.skipBytes(entriesByteSize);
	  }
      }

      // docs are d-gapped, counts are not.
      currentDocument += documents.readInt();
      currentCount = documents.readInt();

      // Prep the extents
      extentArray.clear();
      positionsLoaded = false;
      entryList.clear();
      entriesLoaded = false;
      if (currentCount > inlineMinimum) {
        positionsByteSize = positions.readInt();
	entriesByteSize = entries.readInt();
      } else {
        // Load them aggressively since we can't skip them
        loadPositions();
	loadEntries();
      }
    }

    // Loads up a single set of positions for an intID. Basically it's the
    // load that needs to be done when moving forward one in the posting list.
    private void loadPositions() throws IOException {
      if (!positionsLoaded) {
        int position = 0;
        for (int i = 0; i < currentCount; ++i) {
          position += positions.readInt();
          extentArray.add(position);
        }
        positionsLoaded = true;
      }
    }

    // Loads up a single set of entries for a doc ID.
    private void loadEntries() throws IOException {
	if (!entriesLoaded) {
	    entryList.clear();
	    for (int i = 0; i < currentCount; ++i) {
		int numWords = entries.readInt();
		ArrayList<String> wordList = new ArrayList(numWords);
		for (int j = 0; j < numWords; ++j) {
		    int wordSize = entries.readInt();
		    byte[] wordBuffer = new byte[wordSize];
		    entries.readFully(wordBuffer);
		    wordList.add(Utility.toString(wordBuffer));
		}
		entryList.add(wordList);
	    }
	    entriesLoaded = true;
	}
    }

    @Override
    public String getEntry() throws IOException {
      StringBuilder builder = new StringBuilder();

      builder.append(Utility.toString(key()));
      builder.append(",");
      builder.append(currentDocument);
      ExtentArray e = extents();
      for (int i = 0; i < e.position; ++i) {
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
      extentArray.clear();
      entryList.clear();
      initialize();
    }

    @Override
    public void movePast(int document) throws IOException {
      syncTo(document + 1);
    }

    @Override
    public void syncTo(int document) throws IOException {
      // Linear only for now
      while (!isDone() && document > currentDocument) {
        documentIndex = Math.min(documentIndex + 1, documentCount);
        if (!isDone()) {
          loadNextPosting();
        }
      }
    }

    @Override
    public boolean isDone() {
      return documentIndex >= documentCount;
    }

    @Override
    public List<List<String>> getData() {
	try {
	    loadEntries();
	    return entryList;
	} catch (IOException ioe) {
	  throw new RuntimeException(ioe);
	}
    }

    @Override
    public ExtentArray extents() {
      try {
        loadPositions();
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
