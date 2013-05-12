// BSD License (http://lemurproject.org/galago-license)

package julien.galago.tupleflow;

import java.io.IOException;

import julien.galago.tupleflow.execution.ErrorStore;
import julien.galago.tupleflow.execution.Verification;

/**
 *
 * @author trevor
 */
public class NullSource<T> implements ExNihiloSource<T> {
    public Processor<T> processor;
    Class<T> outputClass;

    public NullSource(TupleFlowParameters parameters) throws ClassNotFoundException {
        String className = parameters.getJSON().getString("class");
        this.outputClass = (Class<T>) Class.forName(className);
    }

    public NullSource(Class<T> outputClass) {
        this.outputClass = outputClass;
    }

    public static void verify(TupleFlowParameters parameters, ErrorStore handler) {
        Verification.requireParameters(new String[]{"class"}, parameters.getJSON(), handler);
        Verification.requireClass(parameters.getJSON().getString("class"), handler);
    }

    public Step setProcessor(Step processor) throws IncompatibleProcessorException {
        Linkage.link(this, processor);
        return processor;
    }

    public void run() throws IOException {
        processor.close();
    }

    public static String getOutputClass(TupleFlowParameters parameters) {
        return parameters.getJSON().get("class", "");
    }
}
