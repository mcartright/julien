// BSD License (http://lemurproject.org/galago-license)
package julien.galago.tupleflow.execution;

/**
 *
 * @author trevor
 */
public interface ErrorHandler {
    public void addError(String errorString);
    public void addWarning(String warningString);
}
