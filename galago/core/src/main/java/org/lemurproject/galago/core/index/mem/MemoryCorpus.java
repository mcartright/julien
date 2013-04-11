// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import java.io.IOException;
import java.util.TreeMap;
import org.lemurproject.galago.core.index.KeyIterator;
import org.lemurproject.galago.core.index.KeyToListIterator;
import org.lemurproject.galago.core.index.corpus.CorpusFileWriter;
import org.lemurproject.galago.core.index.corpus.DocumentReader;
import org.lemurproject.galago.core.index.corpus.DocumentReader.DocumentIterator;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.index.Iterator;
import org.lemurproject.galago.core.index.DataIterator;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.Utility.ByteArrComparator;

public class MemoryCorpus implements DocumentReader, MemoryIndexPart {

  private TreeMap<byte[], Document> corpusData;
  private Parameters params;
  private long docCount;
  private long termCount;

  public MemoryCorpus(Parameters params) throws IOException {
    this.params = params;
    this.corpusData = new TreeMap(new ByteArrComparator());
  }

  @Override
  public void addDocument(Document doc) {

    docCount += 1;
    termCount += doc.terms.size();

    // save a subset of the document 
    // - to match the output of themake-corpus function.
    corpusData.put(Utility.fromInt(doc.identifier), doc);
  }

  // this is likely to waste all of your memory...
  @Override
  public void addIteratorData(byte[] key, Iterator iterator) throws IOException {
    while (!iterator.isDone()) {
      Document doc = ((DataIterator<Document>) iterator).getData();
      // if the document already exists - no harm done.
      addDocument(doc);
      iterator.movePast(iterator.currentCandidate());
    }
  }

  @Override
  public void removeIteratorData(byte[] key) throws IOException {
    throw new IOException("Can not remove Document Names iterator data");
  }

  @Override
  public void close() throws IOException {
    // clean up data.
    corpusData = null;
  }

  public KeyIterator keys() throws IOException {
    return new MemDocumentIterator(corpusData.keySet().iterator());
  }

  @Override
  public Document getDocument(byte[] key, Parameters p) throws IOException {
    return corpusData.get(key);
  }

  @Override
  public Document getDocument(int key, Parameters p) throws IOException {
    return corpusData.get(Utility.fromInt(key));
  }

  @Override
  public Parameters getManifest() {
    return params;
  }

  @Override
  public long getDocumentCount() {
    return docCount;
  }

  @Override
  public long getCollectionLength() {
    return termCount;
  }

  @Override
  public long getKeyCount() {
    return this.corpusData.size();
  }

  @Override
  public void flushToDisk(String path) throws IOException {
    Parameters p = getManifest();
    p.set("filename", path);
    CorpusFileWriter writer = new CorpusFileWriter(new FakeParameters(p));
    DocumentIterator iterator = (DocumentIterator) keys();
    while (!iterator.isDone()) {
      writer.process(iterator.getDocument(new Parameters()));
      iterator.nextKey();
    }
    writer.close();
  }

  @Override
  public Iterator getIterator(byte[] key) throws IOException {
    String asStr = Utility.toString(key);
    if ("corpus".equals(asStr)) {
      return new MemCorpusIterator(keys());
    } else {
      throw new UnsupportedOperationException(
              String.format(
              "Index doesn't support operator: %s", asStr));
    }
  }

  // document iterator
  public class MemDocumentIterator implements DocumentIterator {

    private java.util.Iterator<byte[]> keyIterator;
    private byte[] currKey;

    public MemDocumentIterator(java.util.Iterator<byte[]> iterator) throws IOException {
      this.keyIterator = iterator;
      nextKey();
    }

    @Override
    public boolean skipToKey(byte[] key) throws IOException {
      keyIterator = corpusData.tailMap(key).keySet().iterator();
      nextKey();
      return (Utility.compare(key, currKey) == 0);
    }

    @Override
    public boolean findKey(byte[] key) throws IOException {
      keyIterator = corpusData.tailMap(key).keySet().iterator();
      nextKey();
      return (Utility.compare(key, currKey) == 0);
    }

    @Override
    public String getKeyString() {
      return Integer.toString(Utility.toInt(currKey));
    }

    @Override
    public byte[] getKey() {
      return currKey;
    }

    @Override
    public boolean isDone() {
      return currKey == null;
    }

    @Override
    public Document getDocument(Parameters p) throws IOException {
      return corpusData.get(currKey);
    }

    @Override
    public boolean nextKey() throws IOException {
      if (keyIterator.hasNext()) {
        currKey = keyIterator.next();
        return true;
      } else {
        currKey = null;
        return false;
      }
    }

    @Override
    public String getValueString() throws IOException {
      return getDocument(new Parameters()).toString();
    }

    @Override
    public void reset() throws IOException {
      keyIterator = corpusData.keySet().iterator();
      nextKey();
    }

    @Override
    public int compareTo(KeyIterator t) {
      try {
        return Utility.compare(this.getKey(), t.getKey());
      } catch (IOException ex) {
        throw new RuntimeException("Failed to compare mem-corpus keys");
      }
    }

    // unsupported functions:
    @Override
    public Iterator getValueIterator() throws IOException {
      return new MemCorpusIterator(this);
    }

    @Override
    public byte[] getValueBytes() throws IOException {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }

  public class MemCorpusIterator
          extends KeyToListIterator
          implements DataIterator<Document> {

    Parameters docParams;

    public MemCorpusIterator(KeyIterator ki) {
      super(ki);
      docParams = new Parameters();
    }

    @Override
    public String getEntry() throws IOException {
      return ((KeyIterator) iterator).getValueString();
    }

    @Override
    public long totalEntries() {
      return docCount;
    }

    @Override
    public Document getData() {
      try {
        return ((MemDocumentIterator) iterator).getDocument(docParams);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    @Override
    public boolean hasAllCandidates() {
      return true;
    }

    @Override
    public byte[] key() {
      try {
        return this.iterator.getKey();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }
}
