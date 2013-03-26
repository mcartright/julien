/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.geometric;

import java.io.IOException;
import java.util.Collection;
import java.util.PriorityQueue;
import org.lemurproject.galago.core.index.Iterator;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public abstract class DisjointIndexesIterator implements Iterator {

  Collection<Iterator> allIterators;
  Iterator head;
  PriorityQueue<Iterator> queue;

  public DisjointIndexesIterator(Collection<Iterator> iterators) {
    allIterators = iterators;
    queue = new PriorityQueue(iterators);
    head = queue.poll();
  }
  
  @Override
  public boolean isDone() {
    return queue.isEmpty() && head.isDone();
  }

  @Override
  public int currentCandidate() {
    return head.currentCandidate();
  }

  @Override
  public boolean hasMatch(int identifier) {
    return (head.currentCandidate() == identifier);
  }

  @Override
  public void movePast(int identifier) throws IOException {
    syncTo(identifier + 1);
  }

  @Override
  public void syncTo(int identifier) throws IOException {
    queue.offer(head);
    while (!queue.isEmpty()) {
      head = queue.poll();
      head.syncTo(identifier);
      if (queue.isEmpty()) {
        // if the queue is empty - we're done
        return;
      } else if (!head.isDone()
              && !queue.isEmpty()
              && head.compareTo(queue.peek()) < 0) {
        // otherwise check if head is still the head
        return;
      }

      // if we are here - head may be done - or may not be the head anymore
      if (!head.isDone()) {
        queue.offer(head);
      }
    }
  }

  @Override
  public String getEntry() throws IOException {
    return head.getEntry();
  }

  @Override
  public long totalEntries() {
    long count = 0;
    for (Iterator i : allIterators) {
      count += i.totalEntries();
    }
    return count;
  }

  @Override
  public void reset() throws IOException {
    queue = new PriorityQueue();
    for (Iterator i : allIterators) {
      i.reset();
      queue.offer(i);
    }
    while (!queue.isEmpty()) {
      head = queue.poll();
      if (!head.isDone()) {
        return;
      }
    }
  }

  @Override
  public boolean hasAllCandidates() {
    boolean flag = true;
    for (Iterator i : allIterators) {
      flag &= i.hasAllCandidates();
    }
    return flag;
  }

  @Override
  public int compareTo(Iterator o) {
    return Utility.compare(this.currentCandidate(), o.currentCandidate());
  }

  @Override
  public byte[] key() {
    return ((Iterator) head).key();
  }
}
