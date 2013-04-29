// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index;

import julien.galago.core.util.ExtentArray;

/**
 * This describes an iterator that returns extents - actual spans of text
 * that denote a region in the underlying document.
 *
 * @author irmarc
 */
public interface ExtentIterator extends CountIterator {
    public ExtentArray extents();
}
