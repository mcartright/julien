// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index.disk;

import java.io.IOException;

import julien.galago.core.types.FieldNumberWordPosition;
import julien.galago.core.types.NumberWordPosition;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.Processor;
import julien.galago.tupleflow.TupleFlowParameters;
import julien.galago.tupleflow.execution.ErrorStore;
import julien.galago.tupleflow.execution.Verification;


/**
 *
 * @author jykim
 */
@InputClass(className = "julien.galago.core.types.FieldNumberWordPosition", order = {"+field", "+word", "+document", "+position"})
public class PositionFieldIndexWriter implements Processor<FieldNumberWordPosition> {

  private NumberWordPosition.WordDocumentPositionOrder.TupleShredder shredder;
  private PositionIndexWriter writer;
  private String filePrefix;
  private String prevField;
  private final TupleFlowParameters p;

  public PositionFieldIndexWriter(TupleFlowParameters p) throws IOException {
    this.p = p;
    filePrefix = p.getJSON().getString("filename");
  }

  private void checkWriter(String fieldName) throws IOException {
    if (prevField == null || prevField != fieldName) {
      if (shredder != null) {
        shredder.close();
      }
      p.getJSON().set("filename",
		      String.format("%s%s.postings", filePrefix, fieldName));
      writer = new PositionIndexWriter(p);
      shredder = new NumberWordPosition.WordDocumentPositionOrder.TupleShredder(writer);
      prevField = fieldName;
    }
  }

  public void process(FieldNumberWordPosition object) throws IOException {
    checkWriter(object.field);
    shredder.process(new NumberWordPosition(object.document, object.word, object.position));
  }

  public void close() throws IOException {

    if (shredder != null) {
      shredder.close();
    }
  }

  public static void verify(TupleFlowParameters parameters, ErrorStore handler) {
    if (!parameters.getJSON().isString("filename")) {
      handler.addError("PositionFieldIndexWriter requires a 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableDirectory(index, handler);
  }
}
