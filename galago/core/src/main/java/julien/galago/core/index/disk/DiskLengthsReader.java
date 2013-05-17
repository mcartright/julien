// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index.disk;

import java.io.FileNotFoundException;
import java.io.IOException;

import julien.galago.core.index.*;
import julien.galago.core.index.AggregateReader.CollectionStatistics;
import julien.galago.core.index.BTreeReader.BTreeIterator;
import julien.galago.tupleflow.DataStream;
import julien.galago.tupleflow.Utility;


/**
 * Reads documents lengths from a document lengths file. KeyValueIterator
 * provides a useful interface for dumping the contents of the file.
 *
 * data stored in each document 'field' lengths list:
 *
 * stats: - number of non-zero document lengths (document count) - sum of
 * document lengths (collection length) - average document length - maximum
 * document length - minimum document length
 *
 * utility values: - first document id - last document id (all documents
 * inbetween have a value)
 *
 * finally: - list of lengths (one per document)
 *
 * @author irmarc
 * @author sjh
 */
public class DiskLengthsReader extends KeyListReader implements LengthsReader {

  // this is a special memory map for document lengths
  // it is used in the special documentLengths iterator
  private byte[] doc;
//  private MappedByteBuffer documentLengths;
//  private MemoryMapLengthsIterator documentLengthsIterator;

  public DiskLengthsReader(String filename) throws FileNotFoundException, IOException {
    super(filename);
    init();
  }

  public DiskLengthsReader(BTreeReader r) throws IOException {
    super(r);
    init();
  }

  public void init() throws IOException {
    if (!reader.getManifest().get("emptyIndexFile", false)) {
      doc = Utility.fromString("all");
    }
  }

  @Override
  public int getLength(int document) throws IOException {
    LengthsIterator i = getLengthsIterator();
    ((Iterator) i).syncTo(document);
    // will return either the currect length or a zero if no match.
    return i.getCurrentLength();
  }

  @Override
  public KeyIterator keys() throws IOException {
    return new KeyIterator(reader);
  }

  @Override
  public LengthsIterator getLengthsIterator() throws IOException {
    BTreeIterator i = reader.getIterator(doc);
    return new StreamLengthsIterator(doc, i);
  }

  // Default this puppy to "all" to get the document-scale lengths
  @Override
  public Iterator getIterator(byte[] key) throws IOException {
    BTreeIterator i = reader.getIterator(key);
    return new StreamLengthsIterator(key, i);
  }

  public class KeyIterator extends KeyListReader.KeyValueIterator {

    public KeyIterator(BTreeReader reader) throws IOException {
      super(reader);
    }

    @Override
    public String getValueString() {
      return "length Data";
    }

    @Override
    public Iterator getValueIterator() throws IOException {
      return getStreamValueIterator();
    }

    public StreamLengthsIterator getStreamValueIterator() throws IOException {
      return new StreamLengthsIterator(iterator.getKey(), iterator);
    }

    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(getKey());
    }
  }

  public class StreamLengthsIterator extends KeyListReader.ListIterator
          implements CountIterator, LengthsIterator,
          AggregateReader.CollectionAggregateIterator {

    private final BTreeIterator iterator;
    private DataStream streamBuffer;
    // stats
    private long nonZeroDocCount;
    private long documentCount;
    private long collectionLength;
    private double avgLength;
    private long maxLength;
    private long minLength;
    // utility
    private int firstDocument;
    private int lastDocument;
    // iteration vars
    private int currDocument;
    private int currLength;
    private long lengthsDataOffset;
    private boolean done;

    public StreamLengthsIterator(byte[] key, BTreeIterator it) throws IOException {
      super(key);
      this.iterator = it;
      reset(it);
    }

    @Override
    public void reset(BTreeIterator it) throws IOException {
      super.reset(it);
      this.streamBuffer = it.getValueStream();

      // collect stats
      this.nonZeroDocCount = streamBuffer.readLong();
      this.documentCount = streamBuffer.readLong();
      this.collectionLength = streamBuffer.readLong();
      this.avgLength = streamBuffer.readDouble();
      this.maxLength = streamBuffer.readLong();
      this.minLength = streamBuffer.readLong();
      this.firstDocument = streamBuffer.readInt();
      this.lastDocument = streamBuffer.readInt();

      this.lengthsDataOffset = this.streamBuffer.getPosition(); // should be == (4 * 6) + (8)

      // offset is the first document
      this.currDocument = firstDocument;
      this.currLength = -1;
      this.done = (currDocument > lastDocument);
    }

    @Override
    public void reset() throws IOException {
      this.reset(iterator);
    }

    @Override
    public int currentCandidate() {
      return this.currDocument;
    }

    @Override
    public boolean hasAllCandidates() {
      return true;
    }

    @Override
    public boolean syncTo(int identifier) throws IOException {
      assert identifier >= currDocument : String.format("current doc: %d, syncing to: %d\n",
              currDocument, identifier);

      // we can't move past the last document
      if (identifier > lastDocument) {
        done = true;
        identifier = lastDocument;
      }

      if (currDocument < identifier) {
        // we only delete the length if we move
        // this is because we can't re-read the length value
        currDocument = identifier;
        currLength = -1;
      }
      return currDocument == identifier;
    }

    @Override
    public int movePast(int identifier) throws IOException {
      // select the next document:
      identifier += 1;

      assert (identifier >= currDocument);

      // we can't move past the last document
      if (identifier > lastDocument) {
        done = true;
        identifier = lastDocument;
      }

      if (currDocument < identifier) {
        // we only delete the length if we move
        // this is because we can't re-read the length value
        currDocument = identifier;
        currLength = -1;
      }
      return currDocument;
    }

    @Override
    public boolean isDone() {
      return done;
    }

    @Override
    public String getEntry() throws IOException {
      return getCurrentIdentifier() + "," + getCurrentLength();
    }

    @Override
    public long totalEntries() {
      return this.documentCount;
    }

    @Override
    public int count() {
      return getCurrentLength();
    }

    @Override
    public int getCurrentLength() {
      // check if we need to read the length value from the stream
      if (this.currLength < 0) {
        // ensure a defaulty value
        this.currLength = 0;
        // check for range.
        if (firstDocument <= currDocument && currDocument <= lastDocument) {
          // seek to the required length - hopefully this will hit cache
          this.streamBuffer.seek(lengthsDataOffset + (4 * (this.currDocument - firstDocument)));
          try {
            this.currLength = this.streamBuffer.readInt();
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        }
      }
      return currLength;
    }

    @Override
    public int maximumCount() {
      return Integer.MAX_VALUE;
    }

    @Override
    public int getCurrentIdentifier() {
      return this.currDocument;
    }

    @Override
    public byte[] getRegionBytes() {
      return this.key;
    }

    @Override
    public CollectionStatistics getStatistics() {
      CollectionStatistics cs = new CollectionStatistics();
      cs.fieldName = Utility.toString(key);
      cs.collectionLength = this.collectionLength;
      cs.nonZeroDocCount = this.nonZeroDocCount;
      cs.documentCount = this.documentCount;
      cs.maxLength = this.maxLength;
      cs.minLength = this.minLength;
      cs.avgLength = this.avgLength;
      return cs;
    }
  }
}
