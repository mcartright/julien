// BSD License (http://lemurproject.org/galago-license)

package julien.galago.core.parse;

import java.io.IOException;
import java.util.HashSet;

import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.Parameters;
import julien.galago.tupleflow.StandardStep;
import julien.galago.tupleflow.TupleFlowParameters;

/**
 *
 * @author trevor
 */

@InputClass(className="julien.galago.core.parse.Document")
@OutputClass(className="julien.galago.core.parse.Document")
public class DocumentFilter extends StandardStep<Document, Document> {
    HashSet<String> docnos = new HashSet();
    
    /** Creates a new instance of DocumentFilter */
    public DocumentFilter(TupleFlowParameters parameters) {
        Parameters p = parameters.getJSON();
        docnos.addAll(p.getList("identifier"));
    }
    
    public void process(Document document) throws IOException {
        if (docnos.contains(document.name))
            processor.process(document);
    }
    
    public Class<Document> getOutputClass() {
        return Document.class;
    }
    
    public Class<Document> getInputClass() {
        return Document.class;
    }
}
