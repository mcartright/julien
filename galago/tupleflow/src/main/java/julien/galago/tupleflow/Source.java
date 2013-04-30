// BSD License (http://lemurproject.org/galago-license)

package julien.galago.tupleflow;

/**
 * An object that can generate objects of type T
 * @author trevor
 */
public interface Source<T> extends Step {
    public Step setProcessor(Step processor) throws IncompatibleProcessorException;
}
