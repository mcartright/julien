// BSD License (http://lemurproject.org/galago-license)
/*
 * NullExtentIteratorTest.java
 * JUnit based test
 *
 * Created on September 13, 2007, 6:56 PM
 */
package org.lemurproject.galago.core.index;

import junit.framework.*;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class NullExtentIteratorTest extends TestCase {

  public NullExtentIteratorTest(String testName) {
    super(testName);
  }

  public void testIsDone() {
    NullExtentIterator instance = 
            new NullExtentIterator(Utility.fromString("none"));
    assertEquals(true, instance.isDone());
  }
}
