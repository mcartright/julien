// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index.mem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import julien.galago.core.index.DataIterator;
import julien.galago.core.index.Iterator;
import julien.galago.core.index.KeyIterator;
import julien.galago.core.index.KeyToListIterator;
import julien.galago.core.index.NamesReader;
import julien.galago.core.index.disk.DiskNameReverseWriter;
import julien.galago.core.index.disk.DiskNameWriter;
import julien.galago.core.parse.Document;
import julien.galago.core.types.NumberedDocumentData;
import julien.galago.core.util.ObjectArray;
import julien.galago.tupleflow.DataStream;
import julien.galago.tupleflow.FakeParameters;
import julien.galago.tupleflow.Parameters;
import julien.galago.tupleflow.Utility;


public class MemoryDocumentNames implements MemoryIndexPart, NamesReader {

  private ObjectArray<String> names = new ObjectArray(String.class, 256);
  private int offset;
  private Parameters params;
  private long docCount;
  private long termCount;

  public MemoryDocumentNames(Parameters params) {
    this.params = params;
    this.params.set("writerClass", "julien.galago.core.index.DocumentNameWriter");
  }

  public void addDocument(Document doc) {
    if (names.getPosition() == 0) {
      offset = doc.identifier;
    }
    assert (names.getPosition() + offset == doc.identifier);

    docCount += 1;
    termCount += doc.terms.size();
    names.add(doc.name);
  }

  @Override
  public void addIteratorData(byte[] key, Iterator iterator) throws IOException {
    while (!iterator.isDone()) {
      int identifier = ((NamesReader.NamesIterator) iterator).getCurrentIdentifier();
      String name = ((NamesReader.NamesIterator) iterator).getCurrentName();

      if (names.getPosition() == 0) {
        offset = identifier;
      }

      if (offset + names.getPosition() > identifier) {
        throw new IOException("Unable to add names data out of order.");
      }

      // if we are adding id + lengths directly - we need
      while (offset + names.getPosition() < identifier) {
        names.add(null);
      }

      docCount += 1;
      termCount += 1;
      names.add(name);
      iterator.movePast(identifier);
    }
  }

  @Override
  public void removeIteratorData(byte[] key) throws IOException {
    throw new IOException("Can not remove Document Names iterator data");
  }

  @Override
  public String getDocumentName(int docNum) {
    int index = docNum - offset;
    assert ((index >= 0) && (index < names.getPosition())) : "Document identifier not found in this index.";
    return names.getBuffer()[index];
  }

  @Override
  public int getDocumentIdentifier(String document) {
    throw new UnsupportedOperationException("Not yet implemented");
  }

  @Override
  public void close() throws IOException {
    names = null;
    offset = 0;
  }

  @Override
  public NamesReader.NamesIterator getNamesIterator() throws IOException {
    return new VIterator(new KIterator());
  }

  @Override
  public KeyIterator keys() throws IOException {
    return new KIterator();
  }

  @Override
  public Iterator getIterator(byte[] key) throws IOException {
    return new VIterator(keys());
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
    return this.names.getPosition();
  }

  @Override
  public void flushToDisk(String path) throws IOException {
    Parameters p = getManifest().clone();
    p.set("filename", path);
    DiskNameWriter writer = new DiskNameWriter(new FakeParameters(p));
    KIterator iterator = new KIterator();
    NumberedDocumentData d;
    ArrayList<NumberedDocumentData> tempList = new ArrayList();
    while (!iterator.isDone()) {
      d = new NumberedDocumentData();
      d.identifier = iterator.getCurrentName();
      d.number = iterator.getCurrentIdentifier();
      writer.process(d);
      tempList.add(d);
      iterator.nextKey();
    }
    writer.close();

    Collections.sort(tempList, new NumberedDocumentData.IdentifierOrder().lessThan());

    p = getManifest().clone();
    p.set("filename", path + ".reverse");
    DiskNameReverseWriter revWriter = new DiskNameReverseWriter(new FakeParameters(p));

    for (NumberedDocumentData ndd : tempList) {
      revWriter.process(ndd);
    }
    revWriter.close();

  }

  public class KIterator implements KeyIterator {

    int current = 0;
    boolean done = false;

    @Override
    public void reset() throws IOException {
      current = 0;
      done = false;
    }

    public int getCurrentIdentifier() {
      return offset + current;
    }

    public String getCurrentName() throws IOException {
      if (current < names.getPosition()) {
        return names.getBuffer()[current];
      } else {
        return "";
      }
    }

    @Override
    public String getKeyString() {
      return Integer.toString(current + offset);
    }

    @Override
    public byte[] getKey() {
      return Utility.fromInt(offset + current);
    }

    @Override
    public boolean nextKey() throws IOException {
      current++;
      if (current >= 0 && current < names.getPosition()) {
        return true;
      }
      current = 0;
      done = true;
      return false;
    }

    @Override
    public boolean skipToKey(byte[] key) throws IOException {
      current = Utility.toInt(key) - offset;
      if (current >= 0 && current < names.getPosition()) {
        return true;
      }
      current = 0;
      done = true;
      return false;
    }

    @Override
    public boolean findKey(byte[] key) throws IOException {
      return skipToKey(key);
    }

    @Override
    public String getValueString() throws IOException {
      return getCurrentName();
    }

    @Override
    public byte[] getValueBytes() throws IOException {
      return Utility.fromString(this.getCurrentName());
    }

    public DataStream getValueStream() throws IOException {
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
      return new VIterator(this);
    }
  }

  public class VIterator extends KeyToListIterator implements DataIterator<String>, NamesReader.NamesIterator {

    public VIterator(KeyIterator ki) {
      super(ki);
    }

    @Override
    public String getEntry() throws IOException {
      KIterator ki = (KIterator) iterator;
      StringBuilder sb = new StringBuilder();
      sb.append(ki.getCurrentIdentifier());
      sb.append(",");
      sb.append(ki.getCurrentName());
      return sb.toString();
    }

    @Override
    public long totalEntries() {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getData() {
      try {
        return getCurrentName();
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    @Override
    public boolean hasAllCandidates() {
      return true;
    }

    public boolean skipToKey(int candidate) throws IOException {
      KIterator ki = (KIterator) iterator;
      return ki.skipToKey(Utility.fromInt(candidate));
    }

    @Override
    public String getCurrentName() throws IOException {
      KIterator ki = (KIterator) iterator;
      return ki.getCurrentName();
    }

    @Override
    public int getCurrentIdentifier() throws IOException {
      KIterator ki = (KIterator) iterator;
      return ki.getCurrentIdentifier();
    }

    @Override
    public byte[] key() {
      return Utility.fromString("keys");
    }
  }
}
