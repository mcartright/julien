/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.tupleflow;

import java.util.HashMap;

/**
 * An in-memory LRU cache of anything. The assigned limit will determine
 * how big the cache is allowed to get. Keys are always strings.
 * 
 * 
 * @author irmarc
 */
public class ItemCache<T> {

  private class AccessOrderNode {

    public String key;
    public T value;
    public AccessOrderNode prev;
    public AccessOrderNode next;
  }
  /**
   * The size the cache can grow to until it starts
   * ejecting items.
   */
  private int cacheLimit;
  /**
   * Actually holds references to the items we want.
   */
  private HashMap<String, AccessOrderNode> itemRef;
  /**
   * Used to enforce the ejection policy (LRU).
   */
  private AccessOrderNode head;
  private AccessOrderNode tail;

  public ItemCache(int limit) {
    cacheLimit = limit;
    itemRef = new HashMap<String, AccessOrderNode>();
  }

  public boolean contains(String key) {
    return itemRef.containsKey(key);
  }

  public void clear() {
    itemRef.clear();
    head = null;
    tail = null;
  }

  public T get(String key) {
    return itemRef.get(key).value;
  }

  public void add(String key, T item) {
    if (itemRef.containsKey(key)) {
      // Only need to update the lru list
      moveToBack(itemRef.get(key));
      return;
    }

    // Cache is full - need to eject
    if (itemRef.size() >= cacheLimit) {
      // first remove the head item, 
      itemRef.remove(head.key);
      // then shift the lru list
      AccessOrderNode tmp = head;
      head = head.next;
      head.prev = null;
      tmp.next = null;
    }

    // Make a new node,
    AccessOrderNode aon = new AccessOrderNode();
    // Set all fields
    aon.key = key;
    aon.value = item;
    aon.next = null;
    aon.prev = tail;
    // make it the new tail
    if (tail != null) {
      tail.next = aon;
    }
    tail = aon;
    // check to see if we need a head
    if (head == null) {
      head = aon;
    }
    // add it to the fast reference
    itemRef.put(key, aon);
  }

  private void moveToBack(AccessOrderNode aon) {
    if (aon == tail) {
      // nothing to do
      return;
    }

    if (aon == head) {
      // shift to a new head (at this point we assume 
      // there's > 1 item in the list)
      AccessOrderNode tmp = head;
      head = head.next;
      head.prev = null;
      tmp.next = null;

      // Now attach to the tail
      tail.next = tmp;
      tmp.prev = tail;
      tail = tmp;
    }
  }
}
