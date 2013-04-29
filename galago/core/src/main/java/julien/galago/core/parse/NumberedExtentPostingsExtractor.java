// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.parse;

import java.io.IOException;

import julien.galago.core.types.FieldNumberWordPosition;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.StandardStep;
import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.execution.Verified;


/**
 *
 * @author jykim
 */
@InputClass(className = "julien.galago.core.parse.Document")
@OutputClass(className = "julien.galago.core.types.FieldNumberWordPosition")
@Verified
public class NumberedExtentPostingsExtractor extends StandardStep<Document, FieldNumberWordPosition> {

  @Override
      public void process(Document object) throws IOException {
      int number = object.identifier;
      for (Tag tag : object.tags) {
	  String field = tag.name;
	  for (int position = tag.begin; position < tag.end; position++) {
	      byte[] word = Utility.fromString(object.terms.get(position));
	      processor.process(new FieldNumberWordPosition(field, number, word, position));
	  }
      }
  }
}
