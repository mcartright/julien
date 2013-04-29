/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package julien.galago.core.parse.stem;

import java.io.IOException;

import julien.galago.core.parse.Document;
import julien.galago.core.types.KeyValuePair;
import julien.galago.tupleflow.*;
import julien.galago.tupleflow.execution.ErrorHandler;


/**
 *
 * @author sjh
 */
@InputClass(className = "julien.galago.core.parse.Document")
@OutputClass(className = "julien.galago.core.types.KeyValuePair")
public class ConflationExtractor extends StandardStep<Document, KeyValuePair> {

  Stemmer stemmer;

  public ConflationExtractor(TupleFlowParameters params) throws Exception {
    String stemmerClass = params.getJSON().getString("stemmerClass");
    stemmer = (Stemmer) Class.forName(stemmerClass).getConstructor().newInstance();
  }

  @Override
  public void process(Document doc) throws IOException {
    for (String term : doc.terms) {
      if (term != null) {
        String stem = stemmer.stem(term);
        if (stem != null) {
          processor.process(new KeyValuePair(Utility.fromString(stem), Utility.fromString(term)));
        }
      }
    }
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getJSON().isString("stemmerClass")) {
      handler.addError(ConflationExtractor.class.getName() + " requires a stemmerClass parameter.");
    } else {
      try {
        String stemmerClass = parameters.getJSON().getString("stemmerClass");
        Object newInstance = Class.forName(stemmerClass).getConstructor().newInstance();
        Stemmer s = (Stemmer) newInstance;
      } catch (Exception e) {
        handler.addError(ConflationExtractor.class.getName() + " failed to get stemmer instance.\n" + e.toString());
      }
    }
  }
}
