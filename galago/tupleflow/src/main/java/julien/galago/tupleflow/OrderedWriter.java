// BSD License (http://lemurproject.org/galago-license)

package julien.galago.tupleflow;

import java.io.IOException;

public abstract class OrderedWriter<T> implements Processor<T> {
    public abstract void process(T object) throws IOException;
}
