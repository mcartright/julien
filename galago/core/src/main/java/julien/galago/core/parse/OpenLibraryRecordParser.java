/*
 * BSD License (http://www.galagosearch.org/license)
 */
package julien.galago.core.parse;

import java.io.BufferedReader;
import java.io.IOException;

import julien.galago.core.parse.Document;
import julien.galago.core.parse.ParserSelector;
import julien.galago.core.types.DocumentSplit;
import julien.galago.tupleflow.Counter;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.Parameters;
import julien.galago.tupleflow.StandardStep;
import julien.galago.tupleflow.TupleFlowParameters;
import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.execution.Verified;
import julien.galago.tupleflow.types.FileName;

/**
 *
 * @author irmarc
 */
@Verified
@InputClass(className = "julien.galago.core.types.DocumentSplit")
@OutputClass(className = "julien.galago.core.parse.Document")
public class OpenLibraryRecordParser extends StandardStep<DocumentSplit, Document> {

  Counter documentsRead, splitsOpened;

  public OpenLibraryRecordParser(TupleFlowParameters parameters) {
    documentsRead = parameters.getCounter("Documents read");
  }

  public void process(DocumentSplit split) throws IOException {
    System.err.printf("Processing input split: %s\n", split.fileName);
    BufferedReader reader = ParserSelector.getBufferedReader(split.fileName, split.isCompressed);
    if (splitsOpened != null) {
      splitsOpened.increment();
    }
    while (reader.ready()) {
      String[] parts = reader.readLine().split("\t");
      String id = parts[1];
      String json = parts[4];
      Document doc = new Document(id, json);
      processor.process(doc);
      if (documentsRead != null) {
	documentsRead.increment();
      }
    }
  }
}
