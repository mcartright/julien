package org.lemurproject.galago.core.index.mem;

import gnu.trove.map.hash.TObjectLongHashMap;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.TreeMap;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.BTreeWriter;
import org.lemurproject.galago.core.index.IndexElement;
import org.lemurproject.galago.tupleflow.DataStream;
import org.lemurproject.galago.tupleflow.MemoryDataStream;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Meant to implement both the BTreeWriter and BTreeReader
 * interfaces. All in memory.
 *
 * @author irmarc
 */
public class MemoryBTree extends BTreeReader implements BTreeWriter {

  private Parameters manifest;
  private TreeMap<byte[], byte[]> btree;
  private TObjectLongHashMap<byte[]> lhash;

  public MemoryBTree() {
    this(new Parameters());
  }

  public MemoryBTree(Parameters p) {
    manifest = p;
    btree = new TreeMap<byte[], byte[]>(new Utility.ByteArrComparator());
    lhash = new TObjectLongHashMap<byte[]>();
  }

  @Override
  public Parameters getManifest() {
    return manifest;
  }

  @Override
  public void add(IndexElement list) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    list.write(baos);
    baos.close();  // just in case?
    btree.put(list.key(), baos.toByteArray());
    // add to the length map - need to update all keys below this one
    byte[] k = btree.lowerKey(list.key());
    if (k != null) {
      for (byte[] newkey = btree.higherKey(k);
              newkey != null;
              newkey = btree.higherKey(k)) {
        long start = lhash.get(k);
        long len = btree.get(k).length;
        lhash.put(newkey, start+len);
        k = newkey;
      }
    } else {
      lhash.put(list.key(), 0L);
    }
  }

  @Override
  public void close() throws IOException {
    // does nothing.
  }

  /**
   * Returns an iterator pointing to the first key in the BTree.
   * @return
   * @throws IOException
   */
  @Override
  public BTreeIterator getIterator() throws IOException {
    return getIterator(btree.firstKey());
  }

  @Override
  public BTreeIterator getIterator(byte[] key) throws IOException {
    assert (btree.containsKey(key));
    return new Iterator(key);
  }

  public class Iterator extends BTreeReader.BTreeIterator {

    private byte[] currentKey;

    private Iterator(byte[] key) {
      currentKey = key;
    }

    @Override
    public byte[] getKey() {
      return currentKey;
    }

    @Override
    public void find(byte[] key) throws IOException {
      currentKey = btree.ceilingKey(key);  // may not be the exact key
    }

    @Override
    public void skipTo(byte[] key) throws IOException {
      currentKey = btree.ceilingKey(key);  // may not be the exact key
    }

    @Override
    public boolean nextKey() throws IOException {
      currentKey = btree.higherKey(currentKey);
      return (currentKey != null);
    }

    @Override
    public boolean isDone() {
      return (currentKey == null);
    }

    @Override
    public long getValueLength() throws IOException {
      assert(btree.containsKey(currentKey));
      byte[] value = btree.get(currentKey);
      return value.length;
    }

    @Override
    public DataStream getValueStream() throws IOException {
      assert(btree.containsKey(currentKey));
      byte[] value = btree.get(currentKey);
      return new MemoryDataStream(value, 0, value.length);
    }

    @Override
    public DataStream getSubValueStream(long offset, long length)
	throws IOException {
      assert(btree.containsKey(currentKey));
      byte[] value = btree.get(currentKey);
      return new MemoryDataStream(value, (int)offset, (int)length);
    }

    /**
     * This value is kept via a parallel btree to avoid having to compute it
     * all on the fly.
     * @return
     * @throws IOException
     */
    @Override
    public long getValueStart() throws IOException {
      assert(btree.containsKey(currentKey));
      return lhash.get(currentKey);
    }

    @Override
    public long getValueEnd() throws IOException {
      return getValueStart() + getValueLength();
    }
  }
}
