/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package julien.galago.core.parse;

import gnu.trove.map.hash.TObjectIntHashMap;
import java.io.IOException;

import julien.galago.core.types.FieldLengthData;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.StandardStep;
import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.execution.Verified;


/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "julien.galago.core.parse.Document")
@OutputClass(className = "julien.galago.core.types.FieldLengthData")
public class FieldLengthExtractor extends StandardStep<Document, FieldLengthData> {

  TObjectIntHashMap<String> fieldLengths = new TObjectIntHashMap();

  @Override
  public void process(Document doc) throws IOException {
    processor.process(new FieldLengthData(Utility.fromString("all"),
					  doc.identifier,
					  doc.terms.size()));

    fieldLengths.clear();
    for (Tag tag : doc.tags) {
      int len = tag.end - tag.begin;
      fieldLengths.adjustOrPutValue(tag.name, len, len);
    }

    for (String field : fieldLengths.keySet()) {
      processor.process(new FieldLengthData(Utility.fromString(field),
					    doc.identifier,
					    fieldLengths.get(field)));
    }
  }
}
