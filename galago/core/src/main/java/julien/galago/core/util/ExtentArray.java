// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.util;

import java.util.Arrays;

/**
 * An array that holds the spans of a particular index key.
 * In most cases, this is just a list of positions (i.e.
 * term positions). However this class is capable of holding
 * spans of (begin, end) - so proper spans are possible.
 *
 * Also iterator behavior has been incorporated into this class.
 * Primarily, the methods 'hasNext', 'next', 'reset', and
 * 'end' (with no parameters) implement this behavior.
 *
 * @author irmarc
 */
public class ExtentArray {
  public static final ExtentArray empty = new ExtentArray();

  private int[] begins;
  private int[] ends;
  public int length;
  private int curPos;


  public ExtentArray(int capacity) {
    begins = new int[capacity];
    ends = null; // lazy load these
    curPos = length = 0;
  }

  public ExtentArray(int[] begins) {
      this.begins = begins;
      length = begins.length;
  }

    public ExtentArray(int[] begins, int[] ends) {
        this.begins = begins;
        this.ends = ends;
        length = begins.length;
    }

  public ExtentArray() {
    this(16);
  }

  private void makeRoom() {
    begins = Arrays.copyOf(begins, begins.length * 2);
    if (ends != null) ends = Arrays.copyOf(ends, ends.length * 2);
  }

  public int capacity() {
    return begins.length;
  }

  public void add(int begin) {
    if (length == begins.length) {
      makeRoom();
    }

    begins[length] = begin;
    length += 1;
  }

  public void add(int begin, int end) {
    if (length == begins.length) {
      makeRoom();
    }

    begins[length] = begin;
    if (ends == null && length == 0) ends = new int[begins.length];
    ends[length] = end;
    length += 1;
  }

  public int begin(int index) {
    return begins[index];
  }

  public int end(int index) {
    if (ends == null) return begins[index]+1;
    return ends[index];
  }

  public void clear() {
    length = 0;
  }

  public String toString(){
    return String.format("ExtentArray:count=%d", length);
  }

  // ITERATOR ITERATOR ITERATOR ITERATOR ITERATOR
  public boolean hasNext() {
      return curPos < length;
  }

  public int next() {
    int toReturn = begins[curPos];
    curPos++;
    return toReturn;
  }

  public int head() {
    return begins[curPos];
  }

  public int end() {
    if (ends == null) return begins[curPos]+1;
    else return ends[curPos];
  }

  public void reset() {
    curPos = 0;
  }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ExtentArray that = (ExtentArray) o;

        if (!activePositionsEqual(begins,
				  length,
				  that.begins,
				  that.length)) return false;
        if (! activePositionsEqual(ends,
				   length,
				   that.ends,
				   that.length)) return false;
        return true;
    }

    private static final boolean activePositionsEqual(int[] a,
						      int a1Length,
						      int[] a2,
						      int a2Length)
    {
        if (a==a2)
            return true;
        if (a==null || a2==null)
            return false;
        if (a2Length != a1Length)
            return false;
        for (int i=0; i<a1Length; i++)
            if (a[i] != a2[i])
                return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = begins != null ? Arrays.hashCode(Arrays.copyOfRange(begins, 0, length)) : 0;
        result = 31 * result + (ends != null ? Arrays.hashCode(Arrays.copyOfRange(ends, 0, length)) : 0);
        result = 31 * result + length;
        return result;
    }
}
