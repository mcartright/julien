// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.parse;

import java.io.IOException;

import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.StandardStep;
import julien.galago.tupleflow.execution.Verified;

/**
 * <p>Sequentially numbers document data objects.</p>
 *
 * <p>The point of this class is to assign small numbers to each document.  This
 * would be simple if only one process was parsing documents, but in fact there are many
 * of them doing the job at once.  So, we extract DocumentData records from each document,
 * put them into a single list, and assign numbers to them.  These NumberedDocumentData
 * records are then used to assign numbers to index postings.
 * </p>
 * 
 * @author trevor
 */
@Verified
@InputClass(className = "julien.galago.core.parse.Document")
@OutputClass(className = "julien.galago.core.parse.Document")
public class SequentialDocumentNumberer extends StandardStep<Document, Document> {

  int curNum = -1;
  int increment = 1;

  @Override
  public void process(Document doc) throws IOException {
    curNum += increment;
    if (doc.identifier < 0) {
      doc.identifier = curNum;
    }
    processor.process(doc);
  }
}
