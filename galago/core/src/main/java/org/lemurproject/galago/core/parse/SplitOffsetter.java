// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.IOException;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;

@Verified
@InputClass(className = "org.lemurproject.galago.core.types.DocumentSplit")
@OutputClass(className = "org.lemurproject.galago.core.types.DocumentSplit")
public class SplitOffsetter extends StandardStep<DocumentSplit, DocumentSplit> {

    int lastEnd = 0;

  @Override
  public void process(DocumentSplit split) throws IOException {
      split.startDocument = lastEnd;
      lastEnd += split.numDocuments;
      processor.process(split);
  }
}
