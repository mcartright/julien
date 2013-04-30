// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.parse;

import java.io.IOException;

import julien.galago.core.types.NumberWordPosition;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.StandardStep;
import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.execution.Verified;


/**
 *
 * @author trevor
 */
@Verified
@InputClass(className = "julien.galago.core.parse.Document")
@OutputClass(className = "julien.galago.core.types.NumberWordPosition")
public class NumberedPostingsPositionExtractor extends StandardStep<Document, NumberWordPosition> {
  public static final int SPACING = 10;
  public int spaces;
  public void process(Document object) throws IOException {
    spaces = 0;
    for (int i = 0; i < object.terms.size(); i++) {
      String term = object.terms.get(i);
      if (term.equals("##")) {
        ++spaces;
        continue;
      }
      if (term == null) {
        continue;
      }

      processor.process(new NumberWordPosition(object.identifier,
                Utility.fromString(term),
                i+(spaces*SPACING)));
    }
  }
}
