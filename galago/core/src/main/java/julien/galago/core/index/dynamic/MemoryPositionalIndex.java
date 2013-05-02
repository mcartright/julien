// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index.dynamic;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import julien.galago.core.index.AggregateReader;
import julien.galago.core.index.CompressedByteBuffer;
import julien.galago.core.index.CountIterator;
import julien.galago.core.index.ExtentIterator;
import julien.galago.core.index.Iterator;
import julien.galago.core.index.KeyIterator;
import julien.galago.core.index.AggregateReader.NodeAggregateIterator;
import julien.galago.core.index.AggregateReader.NodeStatistics;
import julien.galago.core.index.disk.PositionIndexWriter;
import julien.galago.core.parse.Document;
import julien.galago.core.parse.stem.Stemmer;
import julien.galago.core.util.ExtentArray;
import julien.galago.core.util.ExtentArrayIterator;
import julien.galago.tupleflow.FakeParameters;
import julien.galago.tupleflow.Parameters;
import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.VByteInput;
import julien.galago.tupleflow.Utility.ByteArrComparator;



/*
 * author sjh
 *
 * In-memory posting index
 *
 */
public class MemoryPositionalIndex implements MemoryIndexPart, AggregateReader.AggregateIndexPart {

  // this could be a bit big -- but we need random access here
  // perhaps we should use a trie (but java doesn't have one?)
  protected TreeMap<byte[], PositionalPostingList> postings = new TreeMap(new ByteArrComparator());
  protected Parameters parameters;
  protected long collectionDocumentCount = 0;
  protected long collectionPostingsCount = 0;
  protected long vocabCount = 0;
  protected long highestFrequency = 0;
  protected long highestDocumentCount = 0;
  protected Stemmer stemmer = null;

  public MemoryPositionalIndex(Parameters parameters) throws Exception {
    this.parameters = parameters;

    if (parameters.containsKey("stemmer")) {
      stemmer = (Stemmer) Class.forName(parameters.getString("stemmer")).newInstance();
    }

  }

  @Override
  public void addDocument(Document doc) throws IOException {
    int position = 0;
    for (String term : doc.terms) {
      String stem = stemAsRequired(term);
      if (stem != null) {
        addPosting(Utility.fromString(stem), doc.identifier, position);
        position += 1;
      }
    }

    collectionDocumentCount += 1;
    collectionPostingsCount += doc.terms.size();
    vocabCount = postings.size();
  }

  @Override
  public void addIteratorData(byte[] key, Iterator iterator) throws IOException {

    if (postings.containsKey(key)) {
      // do nothing - we have already cached this data
      return;
    }

    PositionalPostingList postingList = new PositionalPostingList(key);
    ExtentIterator mi = (ExtentIterator) iterator;
    while (!mi.isDone()) {
      int document = mi.currentCandidate();
      ExtentArrayIterator ei = new ExtentArrayIterator(mi.extents());
      while (!ei.isDone()) {
        postingList.add(document, ei.currentBegin());
        ei.next();
      }
      mi.movePast(document);
    }

    postings.put(key, postingList);

    this.highestDocumentCount = Math.max(highestDocumentCount, postingList.termDocumentCount);
    this.highestFrequency = Math.max(highestFrequency, postingList.termPostingsCount);
    this.vocabCount = postings.size();
  }

  @Override
  public void removeIteratorData(byte[] key) throws IOException {
    postings.remove(key);
  }

  protected void addPosting(byte[] byteWord, int document, int position) {
    if (!postings.containsKey(byteWord)) {
      PositionalPostingList postingList = new PositionalPostingList(byteWord);
      postings.put(byteWord, postingList);
    }

    PositionalPostingList postingList = postings.get(byteWord);
    postingList.add(document, position);

    // this posting list has changed - check if the aggregate stats also need to change.
    this.highestDocumentCount = Math.max(highestDocumentCount, postingList.termDocumentCount);
    this.highestFrequency = Math.max(highestFrequency, postingList.termPostingsCount);
  }

  // Posting List Reader functions
  @Override
  public KeyIterator keys() throws IOException {
    return new KIterator();
  }

  @Override
  public Iterator getIterator(byte[] key) throws IOException {
    return getTermExtents(key);
  }

  private CountsIterator getTermCounts(byte[] term) throws IOException {
    PositionalPostingList postingList = postings.get(term);
    if (postingList != null) {
      return new CountsIterator(postingList);
    }
    return null;
  }

  private ExtentsIterator getTermExtents(byte[] term) throws IOException {
    PositionalPostingList postingList = postings.get(term);
    if (postingList != null) {
      return new ExtentsIterator(postingList);
    }
    return null;
  }

  // try to free up memory.
  @Override
  public void close() throws IOException {
    // no op
  }

  @Override
  public Parameters getManifest() {
    return parameters;
  }

  @Override
  public long getDocumentCount() {
    return collectionDocumentCount;
  }

  @Override
  public long getCollectionLength() {
    return collectionPostingsCount;
  }

  @Override
  public long getKeyCount() {
    return postings.size();
  }

  @Override
  public void flushToDisk(String path) throws IOException {
    Parameters p = getManifest();
    p.set("filename", path);
    PositionIndexWriter writer = new PositionIndexWriter(new FakeParameters(p));

    KIterator kiterator = new KIterator();
    ExtentsIterator viterator;
    ExtentArray extents;
    while (!kiterator.isDone()) {
      viterator = (ExtentsIterator) kiterator.getValueIterator();
      writer.processWord(kiterator.getKey());

      while (!viterator.isDone()) {
        writer.processDocument(viterator.currentCandidate());
        extents = viterator.extents();
        for (int i = 0; i < extents.size(); i++) {
          writer.processPosition(extents.begin(i));
          writer.processTuple();
        }
        viterator.movePast(viterator.currentCandidate());
      }
      kiterator.nextKey();
    }
    writer.close();
  }

  @Override
  public AggregateReader.IndexPartStatistics getStatistics() {
    AggregateReader.IndexPartStatistics is = new AggregateReader.IndexPartStatistics();
    is.partName = "MemoryPositionIndex";
    is.collectionLength = this.collectionPostingsCount;
    is.vocabCount = this.vocabCount;
    is.highestDocumentCount = this.highestDocumentCount;
    is.highestFrequency = this.highestFrequency;
    return is;
  }

  // private functions
  private String stemAsRequired(String term) {
    if (stemmer != null) {
      return stemmer.stem(term);
    }
    return term;
  }

  // sub classes:
  public class PositionalPostingList {

    byte[] key;
    CompressedByteBuffer documents_cbb = new CompressedByteBuffer();
    CompressedByteBuffer counts_cbb = new CompressedByteBuffer();
    CompressedByteBuffer positions_cbb = new CompressedByteBuffer();
    //IntArray documents = new IntArray();
    //IntArray termFreqCounts = new IntArray();
    //IntArray termPositions = new IntArray();
    int termDocumentCount = 0;
    int termPostingsCount = 0;
    int lastDocument = 0;
    int lastCount = 0;
    int lastPosition = 0;
    int maximumPostingsCount = 0;

    public PositionalPostingList(byte[] key) {
      this.key = key;
    }

    public void add(int document, int position) {
      if (termDocumentCount == 0) {
        // first instance of term
        lastDocument = document;
        lastCount = 1;
        termDocumentCount += 1;
        documents_cbb.add(document);
      } else if (lastDocument == document) {
        // additional instance of term in document
        lastCount += 1;
      } else {
        // new document
        assert lastDocument == 0 || document > lastDocument;
        documents_cbb.add(document - lastDocument);
        lastDocument = document;
        counts_cbb.add(lastCount);
        lastCount = 1;
        termDocumentCount += 1;
        lastPosition = 0;
      }
      assert lastPosition == 0 || position > lastPosition;
      positions_cbb.add(position - lastPosition);
      termPostingsCount += 1;
      lastPosition = position;
      // keep track of the document with the highest frequency of 'term'
      maximumPostingsCount = Math.max(maximumPostingsCount, lastCount);
    }
  }
  // iterator allows for query processing and for streaming posting list data
  // public class Iterator extends ExtentIterator implements IndexIterator {

  public class KIterator implements KeyIterator {

    java.util.Iterator<byte[]> iterator;
    byte[] currKey;
    boolean done = false;

    public KIterator() throws IOException {
      iterator = postings.keySet().iterator();
      this.nextKey();
    }

    @Override
    public void reset() throws IOException {
      iterator = postings.keySet().iterator();
    }

    @Override
    public String getKeyString() throws IOException {
      return Utility.toString(currKey);
    }

    @Override
    public byte[] getKey() {
      return currKey;
    }

    @Override
    public boolean nextKey() throws IOException {
      if (iterator.hasNext()) {
        currKey = iterator.next();
        return true;
      } else {
        currKey = null;
        done = true;
        return false;
      }
    }

    @Override
    public boolean skipToKey(byte[] key) throws IOException {
      iterator = postings.tailMap(key).keySet().iterator();
      return nextKey();
    }

    @Override
    public boolean findKey(byte[] key) throws IOException {
      iterator = postings.tailMap(key).keySet().iterator();
      return nextKey();
    }

    @Override
    public String getValueString() throws IOException {
      long count = -1;
      ExtentsIterator it = new ExtentsIterator(postings.get(currKey));
      count = it.count();
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
    public byte[] getValueBytes() throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isDone() {
      return done;
    }

    @Override
    public int compareTo(KeyIterator t) {
      try {
        return Utility.compare(this.getKey(), t.getKey());
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public Iterator getValueIterator() throws IOException {
      if (currKey != null) {
        return new ExtentsIterator(postings.get(currKey));
      } else {
        return null;
      }
    }
  }

  public class ExtentsIterator
      implements NodeAggregateIterator, ExtentIterator {

    PositionalPostingList postings;
    VByteInput documents_reader;
    VByteInput counts_reader;
    VByteInput positions_reader;
    int iteratedDocs;
    int currDocument;
    int currCount;
    ExtentArray extents;
    ExtentArray emptyExtents;
    boolean done;

    private ExtentsIterator(PositionalPostingList postings) throws IOException {
      this.postings = postings;
      reset();
    }

    @Override
    public void reset() throws IOException {
      documents_reader = new VByteInput(
              new DataInputStream(
              new ByteArrayInputStream(postings.documents_cbb.getBytes())));
      counts_reader = new VByteInput(
              new DataInputStream(
              new ByteArrayInputStream(postings.counts_cbb.getBytes())));
      positions_reader = new VByteInput(
              new DataInputStream(
              new ByteArrayInputStream(postings.positions_cbb.getBytes())));

      done = false;
      iteratedDocs = 0;
      currDocument = 0;
      currCount = 0;
      extents = new ExtentArray();
      emptyExtents = new ExtentArray();

      read();
    }

    @Override
    public int count() {
      return currCount;
    }

    @Override
    public ExtentArray extents() {
      return extents;
    }

    @Override
    public int maximumCount() {
      return Integer.MAX_VALUE;
    }

    @Override
    public boolean isDone() {
      return done;
    }

    @Override
    public int currentCandidate() {
      return currDocument;
    }

    @Override
    public boolean hasMatch(int identifier) {
      return (!isDone() && identifier == currDocument);
    }

    public void loadExtents() throws IOException {
      extents.reset();
      extents.setDocument(currDocument);
      int position = 0;
      for (int i = 0; i < currCount; i++) {
        position += positions_reader.readInt();
        extents.add(position);
      }
    }

    public void read() throws IOException {
      if (iteratedDocs >= postings.termDocumentCount) {
        done = true;
        return;
      } else if (iteratedDocs == postings.termDocumentCount - 1) {
        currDocument = postings.lastDocument;
        currCount = postings.lastCount;
      } else {
        currDocument += documents_reader.readInt();
        currCount = counts_reader.readInt();
      }
      loadExtents();

      iteratedDocs++;
    }

    @Override
    public void syncTo(int identifier) throws IOException {
      // TODO implement skip lists

      while (!isDone() && (currDocument < identifier)) {
        read();
      }
    }

    @Override
    public void movePast(int identifier) throws IOException {

      while (!isDone() && (currDocument <= identifier)) {
        read();
      }
    }

    @Override
    public String getEntry() throws IOException {
      StringBuilder builder = new StringBuilder();

      builder.append(Utility.toString(postings.key));
      builder.append(",");
      builder.append(currDocument);
      for (int i = 0; i < extents.size(); ++i) {
        builder.append(",");
        builder.append(extents.begin(i));
      }

      return builder.toString();
    }

    @Override
    public long totalEntries() {
      return postings.termDocumentCount;
    }

    @Override
    public NodeStatistics getStatistics() {
      NodeStatistics stats = new NodeStatistics();
      stats.node = Utility.toString(postings.key);
      stats.nodeFrequency = postings.termPostingsCount;
      stats.nodeDocumentCount = postings.termDocumentCount;
      stats.maximumCount = postings.maximumPostingsCount;
      return stats;
    }

    @Override
    public int compareTo(Iterator other) {
      if (isDone() && !other.isDone()) {
        return 1;
      }
      if (other.isDone() && !isDone()) {
        return -1;
      }
      if (isDone() && other.isDone()) {
        return 0;
      }
      return currentCandidate() - other.currentCandidate();
    }

    @Override
    public boolean hasAllCandidates() {
      return false;
    }

    @Override
    public byte[] key() {
      return postings.key;
    }
  }

  public class CountsIterator implements NodeAggregateIterator, CountIterator {

    PositionalPostingList postings;
    VByteInput documents_reader;
    VByteInput counts_reader;
    int iteratedDocs;
    int currDocument;
    int currCount;
    boolean done;
    Map<String, Object> modifiers;

    private CountsIterator(PositionalPostingList postings) throws IOException {
      this.postings = postings;
      reset();
    }

    @Override
    public void reset() throws IOException {
      documents_reader = new VByteInput(
              new DataInputStream(
              new ByteArrayInputStream(postings.documents_cbb.getBytes())));
      counts_reader = new VByteInput(
              new DataInputStream(
              new ByteArrayInputStream(postings.counts_cbb.getBytes())));

      iteratedDocs = 0;
      currDocument = 0;
      currCount = 0;

      read();
    }

    @Override
    public int count() {
      return currCount;
    }

    @Override
    public int maximumCount() {
      return Integer.MAX_VALUE;
    }

    @Override
    public boolean isDone() {
      return done;
    }

    @Override
    public int currentCandidate() {
      return currDocument;
    }

    @Override
    public boolean hasMatch(int identifier) {
      return (!isDone() && identifier == currDocument);
    }

    @Override
    public boolean hasAllCandidates() {
      return false;
    }

    public void read() throws IOException {
      if (iteratedDocs >= postings.termDocumentCount) {
        done = true;
        return;
      } else if (iteratedDocs == postings.termDocumentCount - 1) {
        currDocument = postings.lastDocument;
        currCount = postings.lastCount;
      } else {
        currDocument += documents_reader.readInt();
        currCount = counts_reader.readInt();
      }

      iteratedDocs++;
    }

    @Override
    public void syncTo(int identifier) throws IOException {
      // TODO implement skip lists

      while (!isDone() && (currDocument < identifier)) {
        read();
      }
    }

    @Override
    public void movePast(int identifier) throws IOException {

      while (!isDone() && (currDocument <= identifier)) {
        read();
      }
    }

    @Override
    public String getEntry() throws IOException {
      StringBuilder builder = new StringBuilder();

      builder.append(Utility.toString(postings.key));
      builder.append(",");
      builder.append(currDocument);
      builder.append(",");
      builder.append(currCount);

      return builder.toString();
    }

    @Override
    public long totalEntries() {
      return postings.termDocumentCount;
    }

    @Override
    public NodeStatistics getStatistics() {
      if (modifiers != null && modifiers.containsKey("background")) {
        return (NodeStatistics) modifiers.get("background");
      }
      NodeStatistics stats = new NodeStatistics();
      stats.node = Utility.toString(postings.key);
      stats.nodeFrequency = postings.termPostingsCount;
      stats.nodeDocumentCount = postings.termDocumentCount;
      stats.maximumCount = postings.maximumPostingsCount;
      return stats;
    }

    @Override
    public int compareTo(Iterator other) {
      if (isDone() && !other.isDone()) {
        return 1;
      }
      if (other.isDone() && !isDone()) {
        return -1;
      }
      if (isDone() && other.isDone()) {
        return 0;
      }
      return currentCandidate() - other.currentCandidate();
    }

    @Override
    public byte[] key() {
      return postings.key;
    }
  }
}