// BSD License (http://lemurproject.org)

package julien.galago.tupleflow;

/**
 *
 * @author trevor
 */
public interface Type<T> {
    public Order<T> getOrder(String... fields);
}
