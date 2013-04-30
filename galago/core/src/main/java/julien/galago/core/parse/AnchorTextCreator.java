// BSD License (http://lemurproject.org/galago-license)

package julien.galago.core.parse;

import java.io.IOException;

import julien.galago.core.types.AdditionalDocumentText;
import julien.galago.core.types.ExtractedLink;
import julien.galago.tupleflow.Counter;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.StandardStep;
import julien.galago.tupleflow.TupleFlowParameters;
import julien.galago.tupleflow.execution.Verified;


/**
 *
 * @author trevor
 */
@Verified
@InputClass(className = "julien.galago.core.parse.DocumentLinkData")
@OutputClass(className = "julien.galago.core.types.AdditionalDocumentText")
public class AnchorTextCreator extends StandardStep<DocumentLinkData, AdditionalDocumentText> {

    Counter counter;

    public AnchorTextCreator(TupleFlowParameters parameters) {
      counter = parameters.getCounter("Anchors Created");
    }

    @Override
    public void process(DocumentLinkData object) throws IOException {
        AdditionalDocumentText additional = new AdditionalDocumentText();
        StringBuilder extraText = new StringBuilder();

        additional.identifier = object.identifier;
        for (ExtractedLink link : object.links) {
            extraText.append("<anchor>");
            extraText.append(link.anchorText);
            extraText.append("</anchor>");
        }
        additional.text = extraText.toString();

        processor.process(additional);
        if (counter != null) counter.increment();
    }
}
