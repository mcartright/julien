// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index.disk;

import java.io.FileNotFoundException;
import java.io.IOException;

import julien.galago.core.index.GenericElement;
import julien.galago.core.types.KeyValuePair;
import julien.galago.core.types.NumberedDocumentData;
import julien.galago.tupleflow.Counter;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.Parameters;
import julien.galago.tupleflow.Processor;
import julien.galago.tupleflow.Sorter;
import julien.galago.tupleflow.TupleFlowParameters;
import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.execution.ErrorHandler;


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
@InputClass(className = "julien.galago.core.types.NumberedDocumentData", order={"+number"})
public class DiskNameWriter implements Processor<NumberedDocumentData> {

  DiskBTreeWriter writer;
  NumberedDocumentData last = null;
  Counter documentNamesWritten = null;

  public DiskNameWriter(TupleFlowParameters parameters) throws IOException {
    documentNamesWritten = parameters.getCounter("Document Names Written");
    // make a folder
    String filename = parameters.getJSON().getString("filename");

    Parameters p = parameters.getJSON();
    p.set("writerClass", DiskNameWriter.class.getName());
    p.set("readerClass", DiskNameReader.class.getName());
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
    }

    assert last.number <= ndd.number;
    assert ndd.identifier != null;

    writer.add(new GenericElement(
            Utility.fromInt(ndd.number),
            Utility.fromString(ndd.identifier)));

    if (documentNamesWritten != null) {
      documentNamesWritten.increment();
    }
  }

  public void close() throws IOException {
    writer.close();
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getJSON().isString("filename")) {
      handler.addError("DocumentNameWriter requires a 'filename' parameter.");
      return;
    }
  }
}
