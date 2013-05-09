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

        if (length != that.length) return false;
        int[] beginSubset = Arrays.copyOfRange(begins, 0, length);
        int[] otherBegins =  Arrays.copyOfRange(that.begins, 0, that.length);

        int[] endsSubset = Arrays.copyOfRange(ends, 0, length);
        int[] endsBegins =  Arrays.copyOfRange(that.ends, 0, that.length);

        if (!Arrays.equals(beginSubset, otherBegins)) return false;
        if (!Arrays.equals(endsSubset, endsBegins)) return false;

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
