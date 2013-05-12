// BSD License (http://lemurproject.org/galago-license)
package julien.galago.tupleflow.execution;

/**
 * Represents an input step in a TupleFlow stage.
 *
 * @author trevor
 */
public class InputStep extends Step {
    String id;

    public InputStep(String id) {
        this.id = id;
    }

    public String getId() {
        return id;
    }
}
