// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.parse;

import java.io.IOException;

import julien.galago.core.types.NumberedDocumentData;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.Parameters;
import julien.galago.tupleflow.StandardStep;
import julien.galago.tupleflow.execution.Verified;
import julien.galago.tupleflow.types.SerializedParameters;


/**
 *
 * @author trevor
 */
@Verified
@InputClass(className = "julien.galago.core.types.NumberedDocumentData")
@OutputClass(className = "julien.galago.tupleflow.types.SerializedParameters", order = {"+parameters"})
public class CollectionLengthCounter extends StandardStep<NumberedDocumentData, SerializedParameters> {

  long collectionLength = 0;
  long documentCount = 0;

  public void process(NumberedDocumentData data) {
    collectionLength += data.textLength;
    documentCount += 1;
  }

  @Override
  public void close() throws IOException {
    Parameters p = new Parameters();
    p.set("statistics/collectionLength", collectionLength);
    p.set("statistics/documentCount", documentCount);
    processor.process(new SerializedParameters(p.toString()));
    processor.close();
  }
}
