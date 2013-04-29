// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index;

/**
 * Implies an iterator returns a certain type of data
 * @author irmarc
 */
public interface DataIterator<T> extends Iterator {
    public T getData();
}
