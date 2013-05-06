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
  private int position;
  private int iterationPosition;


  public ExtentArray(int capacity) {
    begins = new int[capacity];
    ends = null; // lazy load these
    iterationPosition = position = 0;
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
    if (position == begins.length) {
      makeRoom();
    }

    begins[position] = begin;
    position += 1;
  }

  public void add(int begin, int end) {
    if (position == begins.length) {
      makeRoom();
    }

    begins[position] = begin;
    if (ends == null && position == 0) ends = new int[begins.length];
    ends[position] = end;
    position += 1;
  }

  public int begin(int index) {
    return begins[index];
  }

  public int end(int index) {
    if (ends == null) return begins[index]+1;
    return ends[index];
  }

  public int size() {
    return position;
  }

  public void clear() {
    position = 0;
  }

  public String toString(){
    return String.format("ExtentArray:count=%d", position);
  }

  // ITERATOR ITERATOR ITERATOR ITERATOR ITERATOR
  public boolean hasNext() {
      return iterationPosition < position-1;
  }

  public int next() {
    int toReturn = begins[iterationPosition];
    iterationPosition++;
    return toReturn;
  }

  public int head() {
    return begins[iterationPosition];
  }

  public int end() {
    if (ends == null) return begins[iterationPosition]+1;
    else return ends[iterationPosition];
  }

  public void reset() {
    iterationPosition = 0;
  }
}
