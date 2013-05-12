// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index.disk;

import java.io.IOException;
import java.util.logging.Logger;

import julien.galago.core.index.GenericElement;
import julien.galago.core.types.NumberedDocumentData;
import julien.galago.tupleflow.Counter;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.Parameters;
import julien.galago.tupleflow.Processor;
import julien.galago.tupleflow.TupleFlowParameters;
import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.execution.ErrorStore;


/**
 *
 * Writes a mapping from document names to document numbers
 *
 * Does not assume that the data is sorted
 *  - as data would need to be sorted into both key and value order
 *  - instead this class takes care of the re-sorting
 *  - this may be inefficient, but docnames is a relatively small pair of files
 *
 * @author sjh
 */
@InputClass(className = "julien.galago.core.types.NumberedDocumentData", order = {"+identifier"})
public class DiskNameReverseWriter implements Processor<NumberedDocumentData> {

  DiskBTreeWriter writer;
  NumberedDocumentData last = null;
  Counter documentNamesWritten = null;

  public DiskNameReverseWriter(TupleFlowParameters parameters) throws IOException {
    documentNamesWritten = parameters.getCounter("Document Names Written");
    // make a folder
    String filename = parameters.getJSON().getString("filename");

    Parameters p = parameters.getJSON();
    p.set("writerClass", DiskNameReverseWriter.class.getName());
    p.set("readerClass", DiskNameReverseReader.class.getName());
    if(!p.containsKey("blockSize")) {
      p.set("blockSize", 512);
    }

    writer = new DiskBTreeWriter(filename, p);
  }

  public void process(int number, String identifier) throws IOException {
    byte[] docNum = Utility.fromInt(number);
    byte[] docName = Utility.fromString(identifier);

    writer.add(new GenericElement(docNum, docName));

    if (documentNamesWritten != null) {
      documentNamesWritten.increment();
    }
  }

  public void process(NumberedDocumentData ndd) throws IOException {
    if (last == null) {
      last = ndd;
    } else {
      // ensure that we have an ident
      assert ndd.identifier != null: "DiskNameReverseWriter can not write a null identifier.";
      assert Utility.compare(last.identifier, ndd.identifier) <= 0: "DiskNameReverseWriter wrong order.";
      if(Utility.compare(last.identifier, ndd.identifier) == 0){
        Logger.getLogger(this.getClass().getName()).info("WARNING: identical document names written to names.reverse index");
      }
    }

    writer.add(new GenericElement(
            Utility.fromString(ndd.identifier),
            Utility.fromInt(ndd.number)));

    if (documentNamesWritten != null) {
      documentNamesWritten.increment();
    }
  }

  public void close() throws IOException {
    writer.close();
  }

  public static void verify(TupleFlowParameters parameters, ErrorStore handler) {
    if (!parameters.getJSON().isString("filename")) {
      handler.addError("DocumentNameWriter requires a 'filename' parameter.");
      return;
    }
  }
}
