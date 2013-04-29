 // BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index.corpus;

import java.io.IOException;

import julien.galago.core.index.GenericElement;
import julien.galago.core.parse.Document;
import julien.galago.core.types.KeyValuePair;
import julien.galago.tupleflow.IncompatibleProcessorException;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.Linkage;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.Parameters;
import julien.galago.tupleflow.Processor;
import julien.galago.tupleflow.Source;
import julien.galago.tupleflow.Step;
import julien.galago.tupleflow.TupleFlowParameters;
import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.execution.Verified;


/**
 * Writes documents to a file
 *  - new output file is created in the folder specified by "filename"
 *  - document.identifier -> output-file, byte-offset is passed on
 *
 * @author sjh
 */
@Verified
@InputClass(className = "julien.galago.core.parse.Document")
@OutputClass(className = "julien.galago.core.types.KeyValuePair")
public class CorpusFolderWriter implements Processor<Document>, Source<KeyValuePair> {
  Parameters corpusParams;
  SplitBTreeValueWriter writer;
  boolean useExternalKey;

  public CorpusFolderWriter(TupleFlowParameters parameters) throws IOException, IncompatibleProcessorException {
    corpusParams = parameters.getJSON();
    // create a writer;
    corpusParams.set("writerClass", getClass().getName());
    corpusParams.set("readerClass", CorpusReader.class.getName());
    writer = new SplitBTreeValueWriter(parameters);

    // figure out what the key actually is
    useExternalKey = corpusParams.get("useExternalKey", false);
  }

  @Override
  public void process(Document document) throws IOException {
      if (useExternalKey) {
	  writer.add(new GenericElement(Utility.fromString(document.name),
					Document.serialize(corpusParams, document)));
      } else {
	  writer.add(new GenericElement(Utility.fromInt(document.identifier),
					Document.serialize(corpusParams, document)));
      }
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  @Override
  public Step setProcessor(Step next) throws IncompatibleProcessorException {
    Linkage.link(writer, next);
    return next;
  }
}
