// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index.corpus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import julien.galago.core.parse.Document;
import julien.galago.core.types.KeyValuePair;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.StandardStep;
import julien.galago.tupleflow.TupleFlowParameters;
import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.execution.Verified;

import org.xerial.snappy.SnappyOutputStream;

/**
 * Writes documents to a file - new output file is created in the folder
 * specified by "filename" - document.name -> output-file, byte-offset is passed
 * on
 *
 * @author sjh
 */
@Verified
@InputClass(className = "julien.galago.core.parse.Document")
@OutputClass(className = "julien.galago.core.types.KeyValuePair")
public class DocumentToKeyValuePair extends StandardStep<Document, KeyValuePair> implements KeyValuePair.Source {

  boolean compressed;

  public DocumentToKeyValuePair() {
    compressed = false; // used for testing
  }

  public DocumentToKeyValuePair(TupleFlowParameters parameters) {
    compressed = parameters.getJSON().get("compressed", true);
  }

  public void process(Document document) throws IOException {
    ByteArrayOutputStream array = new ByteArrayOutputStream();
    ObjectOutputStream output;
    if (compressed) {
      output = new ObjectOutputStream(new SnappyOutputStream(array));
    } else {
      output = new ObjectOutputStream(array);
    }

    output.writeObject(document);
    output.close();

    byte[] key = Utility.fromInt(document.identifier);
    byte[] value = array.toByteArray();
    KeyValuePair pair = new KeyValuePair(key, value);
    processor.process(pair);

  }
}
