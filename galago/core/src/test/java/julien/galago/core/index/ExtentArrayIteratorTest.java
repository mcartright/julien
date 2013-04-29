// BSD License (http://lemurproject.org/galago-license)

package julien.galago.core.index;

import julien.galago.core.util.ExtentArray;
import julien.galago.core.util.ExtentArrayIterator;
import junit.framework.*;

/**
 *
 * @author trevor
 */
public class ExtentArrayIteratorTest extends TestCase {
    
    public ExtentArrayIteratorTest(String testName) {
        super(testName);
    }

    public void testEmpty() {
        ExtentArrayIterator instance = new ExtentArrayIterator( new ExtentArray() );
        assertTrue( instance.isDone() );
    }

    public void testSingleExtent() {
        ExtentArray array = new ExtentArray();
        array.setDocument(1);
        array.add( 5, 7 );
        
        ExtentArrayIterator instance = new ExtentArrayIterator( array );
        assertFalse( instance.isDone() );
        assertEquals( instance.getDocument(), 1 );
        assertEquals( instance.currentBegin(), 5 );
        assertEquals( instance.currentEnd(), 7 );
        
        instance.next();
        assertTrue( instance.isDone() );
    }

    public void testTwoExtents() {
        ExtentArray array = new ExtentArray();
        array.setDocument(1);
        array.add( 5, 7 );
        array.add( 9, 11 );
        
        ExtentArrayIterator instance = new ExtentArrayIterator( array );
        assertFalse( instance.isDone() );
        assertEquals( instance.getDocument(), 1 );
        assertEquals( instance.currentBegin(), 5 );
        assertEquals( instance.currentEnd(), 7 );

        instance.next();
        assertFalse( instance.isDone() );
        assertEquals( instance.getDocument(), 1 );
        assertEquals( instance.currentBegin(), 9 );
        assertEquals( instance.currentEnd(), 11 );

        instance.next();
        assertTrue( instance.isDone() );
    }
}
