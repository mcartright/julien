// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.util;

import java.util.Arrays;

/**
 * (9/30/2011) - Refactored to remove useless boxing of the extent data.
 * Saves a ton on object allocation and overall space utilization.
 *
 * @author irmarc
 */
public class ExtentArray {
  public static final ExtentArray empty = new ExtentArray();

  public int[] begins;
  public int[] ends;
  public int position;
  public int document;

  public ExtentArray(int capacity) {
    begins = new int[capacity];
    ends = null; // lazy load these
    position = 0;
    document = -1; // not valid yet
  }

  public ExtentArray() {
    this(16);
  }

  private void makeRoom() {
    begins = Arrays.copyOf(begins, begins.length * 2);
    if (ends != null) ends = Arrays.copyOf(ends, ends.length * 2);
  }

  public void setDocument(int d) {
    document = d;
  }

  public int getDocument() {
    return document;
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

  public void reset() {
    position = 0;
  }

  public String toString(){
    return String.format("ExtentArray:doc=%d:count=%d", document, position);
  }
}
