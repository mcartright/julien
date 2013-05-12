// BSD License (http://lemurproject.org/galago-license)
package julien.galago.tupleflow.execution;

/**
 * Represents an output step in a TupleFlow stage.
 *
 * @author trevor
 */
public class OutputStep extends Step {
    String id;

    public OutputStep(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }
}
