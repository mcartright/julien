// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.parse;

import java.io.IOException;

import julien.galago.core.types.IndexLink;
import julien.galago.tupleflow.Counter;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.StandardStep;
import julien.galago.tupleflow.TupleFlowParameters;
import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.execution.Verified;


@Verified
@InputClass(className = "julien.galago.core.parse.Document")
@OutputClass(className = "julien.galago.core.types.IndexLink")
public class IndexLinkGenerator extends StandardStep<Document, IndexLink> {
    
    private Counter linkCounter;

    public IndexLinkGenerator(TupleFlowParameters parameters) {
	linkCounter = parameters.getCounter("Links Generated");	
    }

    public void process(Document document) throws IOException {
	int linkCount = 0;
	String srcid = document.metadata.get("id");
	String srctype = document.metadata.get("type");
	int srcpos = Integer.parseInt(document.metadata.get("pos"));
	for (Tag tag : document.tags) {
	    IndexLink indexLink = new IndexLink();
	    indexLink.id = Utility.fromString(srcid);
	    indexLink.srctype = srctype;
	    indexLink.pos = srcpos;
	    indexLink.targetid = tag.attributes.get("id");
	    indexLink.targettype = tag.attributes.get("type");
	    indexLink.targetpos = tag.begin;
	    processor.process(indexLink);
	    ++linkCount;
	}
	if (linkCounter != null) linkCounter.incrementBy(linkCount);
    }

    public void close() throws IOException {
	processor.close();
    }
}