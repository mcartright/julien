// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.util.ExtentArray;

/**
 *
 * @author trevor
 * @author irmarc
 */
public class NullExtentIterator implements ExtentIterator {

    ExtentArray array = new ExtentArray();

    public NullExtentIterator() {
    }

    @Override
    public byte[] key() {
        byte[] k = new byte[1];
        k[0] = 0x00;
        return k;
    }

    public boolean nextEntry() {
        return false;
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @Override
    public ExtentArray extents() {
        return array;
    }

    @Override
    public int count() {
        return 0;
    }

    @Override
    public int maximumCount() {
        return 0;
    }

    @Override
    public void reset() {
        // do nothing
    }

    @Override
    public long totalEntries() {
        return 0;
    }

    @Override
    public int currentCandidate() {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean hasMatch(int id) {
        return false;
    }

    @Override
    public String getEntry() throws IOException {
        return "NULL";
    }

    @Override
    public void syncTo(int identifier) throws IOException {
    }

    @Override
    public void movePast(int identifier) throws IOException {
    }

    @Override
    public int compareTo(Iterator t) {
        return 1;
    }

    @Override
    public boolean hasAllCandidates() {
        return false;
    }
}
