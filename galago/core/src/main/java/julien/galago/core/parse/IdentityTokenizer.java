// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.parse;

import java.io.IOException;

import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.StandardStep;
import julien.galago.tupleflow.TupleFlowParameters;
import julien.galago.tupleflow.execution.Verified;

/**
 * A small class to echo the Document as is down the pipeline.
 * This is useful when the parser in fact does all the work already.
 * @author irmarc
 */

@Verified
@InputClass(className = "julien.galago.core.parse.Document")
@OutputClass(className = "julien.galago.core.parse.Document")
public class IdentityTokenizer extends StandardStep<Document, Document> {

  public IdentityTokenizer(TupleFlowParameters parameters) {
  }

  @Override
  public void process(Document document) throws IOException {
    processor.process(document);
  }
}