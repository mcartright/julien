// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.parse;

import java.io.IOException;

import julien.galago.core.types.DocumentSplit;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.StandardStep;
import julien.galago.tupleflow.execution.Verified;


@Verified
@InputClass(className = "julien.galago.core.types.DocumentSplit", order = {"+fileName"})
@OutputClass(className = "julien.galago.core.types.DocumentSplit", order = {"+fileName"})
public class SplitOffsetter extends StandardStep<DocumentSplit, DocumentSplit> {

    int lastEnd = 0;

  @Override
  public void process(DocumentSplit split) throws IOException {
      split.startDocument = lastEnd;
      lastEnd += split.numDocuments;
      System.err.printf("Offsetting: %s\n", split.toString());
      processor.process(split);
  }
}
