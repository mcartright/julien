/*
 * BSD License (http://www.galagosearch.org/license)
 */
package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.core.parse.ParserSelector;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.tupleflow.types.FileName;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;
import java.io.BufferedReader;
import java.io.IOException;

/**
 *
 * @author irmarc
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.DocumentSplit")
@OutputClass(className = "org.lemurproject.galago.core.parse.Document")
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
      doc.fileId = split.fileId;
      doc.totalFileCount = split.totalFileCount;
      processor.process(doc);
      if (documentsRead != null) {
	documentsRead.increment();
      }
    }
  }
}
