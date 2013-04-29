// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index.corpus;

import java.io.FileNotFoundException;
import java.io.IOException;

import julien.galago.core.index.GenericElement;
import julien.galago.core.index.disk.DiskBTreeWriter;
import julien.galago.core.parse.Document;
import julien.galago.tupleflow.Counter;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.Parameters;
import julien.galago.tupleflow.Processor;
import julien.galago.tupleflow.TupleFlowParameters;
import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.execution.ErrorHandler;
import julien.galago.tupleflow.execution.Verification;


/**
 * Writes document text and metadata to an index file.  The output files
 * are in '.corpus' format, which can be fed to UniversalParser as an input
 * to indexing.  The '.corpus' format is also convenient for quickly
 * finding individual documents.
 *
 * @author trevor
 */
@InputClass(className = "julien.galago.core.parse.Document")
public class CorpusFileWriter implements Processor<Document> {

  Parameters corpusParams;
  DiskBTreeWriter writer;
  Counter documentsWritten;

  public CorpusFileWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    corpusParams = parameters.getJSON();
    // create a writer;
    corpusParams.set("writerClass", getClass().getName());
    corpusParams.set("readerClass", CorpusReader.class.getName());
    writer = new DiskBTreeWriter(parameters.getJSON().getString("filename"), corpusParams);
    documentsWritten = parameters.getCounter("Documents Written");
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  @Override
  public void process(Document document) throws IOException {
    writer.add(new GenericElement(Utility.fromInt(document.identifier), Document.serialize(corpusParams, document)));
    if (documentsWritten != null) {
      documentsWritten.increment();
    }
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getJSON().isString("filename")) {
      handler.addError("DocumentIndexWriter requires an 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableFile(index, handler);
  }
}
