// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import org.lemurproject.galago.core.util.ExtentArray;

/**
 * This describes an iterator that returns extents - actual spans of text
 * that denote a region in the underlying document.
 * 
 * @author irmarc
 */
public interface ExtentIterator extends DataIterator<ExtentArray>, CountIterator {
    public ExtentArray extents();
}
