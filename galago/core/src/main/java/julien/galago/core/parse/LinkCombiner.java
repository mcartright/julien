// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.parse;

import java.io.IOException;

import julien.galago.core.types.DocumentData;
import julien.galago.core.types.ExtractedLink;
import julien.galago.core.types.IdentifiedLink;
import julien.galago.core.types.NumberedDocumentData;
import julien.galago.tupleflow.Counter;
import julien.galago.tupleflow.ExNihiloSource;
import julien.galago.tupleflow.IncompatibleProcessorException;
import julien.galago.tupleflow.Linkage;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.Processor;
import julien.galago.tupleflow.Step;
import julien.galago.tupleflow.TupleFlowParameters;
import julien.galago.tupleflow.TypeReader;
import julien.galago.tupleflow.execution.ErrorHandler;
import julien.galago.tupleflow.execution.Verification;


/**
 *
 * @author trevor
 */
@OutputClass(className = "julien.galago.core.parse.DocumentLinkData")
public class LinkCombiner implements ExNihiloSource<IdentifiedLink>, IdentifiedLink.Source {

  TypeReader<ExtractedLink> extractedLinks;
  TypeReader<NumberedDocumentData> documentDatas;
  DocumentLinkData linkData;
  public Processor<DocumentLinkData> processor;
  Counter linksProcessed;

  @SuppressWarnings("unchecked")
  public LinkCombiner(TupleFlowParameters parameters) throws IOException {
    String extractedLinksName = parameters.getJSON().getString("extractedLinks");
    String documentDatasName = parameters.getJSON().getString("documentDatas");
    linksProcessed = parameters.getCounter("Links Combined");
    extractedLinks = parameters.getTypeReader(extractedLinksName);
    documentDatas = parameters.getTypeReader(documentDatasName);
  }

  public Step setProcessor(Step processor) throws IncompatibleProcessorException {
    Linkage.link(this, processor);
    return processor;
  }

  void match(NumberedDocumentData docData, ExtractedLink link) {
    if (linkData == null) {
      linkData = new DocumentLinkData();
      linkData.identifier = docData.number;
      linkData.url = docData.url;
      linkData.textLength = docData.textLength;
    }

    linkData.links.add(link);
  }

  void flush() throws IOException {
    if (linkData != null) {
      processor.process(linkData);
      if (linksProcessed != null) {
        linksProcessed.incrementBy(linkData.links.size());
      }
    }
  }

  public void run() throws IOException {
    ExtractedLink link = extractedLinks.read();
    NumberedDocumentData docData = documentDatas.read();
    while (docData != null && link != null) {
      int result = link.destUrl.toLowerCase().compareTo(docData.url.toLowerCase());
      System.out.println("Comparing destination url: " + link.destUrl + " with doc: "+ docData.url);
      if (result == 0) {
        match(docData, link);
        link = extractedLinks.read();
      } else {
        if (result < 0) {
          link = extractedLinks.read();
        } else {
          flush();
          docData = documentDatas.read();
        }
      }
    }
    flush();
    processor.close();
  }

  public Class<IdentifiedLink> getOutputClass() {
    return IdentifiedLink.class;
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!Verification.requireParameters(new String[]{"extractedLinks", "documentDatas"},
            parameters.getJSON(), handler)) {
      return;
    }

    String extractedLinksName = parameters.getJSON().getString("extractedLinks");
    String documentDatasName = parameters.getJSON().getString("documentDatas");

    //Verification.verifyTypeReader(extractedLinksName, ExtractedLink.class, parameters, handler);
    //Verification.verifyTypeReader(documentDatasName, DocumentData.class, parameters, handler);
  }
}
