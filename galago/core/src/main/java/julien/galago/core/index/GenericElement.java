// BSD License (http://lemurproject.org/galago-license)

package julien.galago.core.index;

import java.io.IOException;
import java.io.OutputStream;

import julien.galago.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class GenericElement implements IndexElement {
    byte[] key;
    byte[] data;

    public GenericElement(byte[] key, byte[] data) {
        this.key = key;
        this.data = data;
    }
    
    public GenericElement(String key, byte[] data) {
        this.key = Utility.fromString(key);
        this.data = data;
    }
    
    public GenericElement(String key, String value) {
        this.key = Utility.fromString(key);
        this.data = Utility.fromString(value);
    }

    public byte[] key() {
        return key;
    }

    public long dataLength() {
        return data.length;
    }

    public void write(OutputStream stream) throws IOException {
        stream.write(data);
    }
}