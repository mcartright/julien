// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index.corpus;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import julien.galago.core.index.GenericElement;
import julien.galago.core.index.disk.DiskBTreeWriter;
import julien.galago.core.parse.Document;
import julien.galago.core.parse.PseudoDocument;
import julien.galago.core.types.KeyValuePair;
import julien.galago.tupleflow.Counter;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.Parameters;
import julien.galago.tupleflow.TupleFlowParameters;
import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.execution.ErrorHandler;
import julien.galago.tupleflow.execution.Verification;

import org.xerial.snappy.SnappyInputStream;

@InputClass(className = "julien.galago.core.types.KeyValuePair", order = {"+key"})
public class DocumentAggregator implements KeyValuePair.KeyOrder.ShreddedProcessor {

  Counter docsIn, docsOut;
  DiskBTreeWriter writer;
  int documentNumber = 0;
  byte[] lastIdentifier = null;
  Map<String, PseudoDocument> bufferedDocuments;

  public DocumentAggregator(TupleFlowParameters parameters) throws IOException, FileNotFoundException {
    docsIn = parameters.getCounter("Documents in");
    docsOut = parameters.getCounter("Documents out");
    Parameters corpusParams = parameters.getJSON();
    // create a writer;
    corpusParams.set("writerClass", getClass().getName());
    corpusParams.set("readerClass", CorpusReader.class.getName());
    writer = new DiskBTreeWriter(parameters);
    bufferedDocuments = new HashMap<String, PseudoDocument>();
  }

  public void processKey(byte[] key) throws IOException {
    if (lastIdentifier == null
            || Utility.compare(key, lastIdentifier) != 0) {
      if (lastIdentifier != null) {
        write();
      }
      lastIdentifier = key;
    }
  }

  public void processTuple(byte[] value) throws IOException {
    ByteArrayInputStream stream = new ByteArrayInputStream(value);
    Document document;
    try {
      ObjectInputStream input = new ObjectInputStream(new SnappyInputStream(stream));
      document = (Document) input.readObject();
      addToBuffer(document);
      if (docsIn != null) {
        docsIn.increment();
      }
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(cnfe);
    }
  }

  private void addToBuffer(Document d) {
    if (!bufferedDocuments.containsKey(d.name)) {
      bufferedDocuments.put(d.name, new PseudoDocument(d));
    } else {
      bufferedDocuments.get(d.name).addSample(d);
    }
  }

  private Parameters emptyParameters = new Parameters();
  private void write() throws IOException {
    for (String nameKey : bufferedDocuments.keySet()) {
      ByteArrayOutputStream array = new ByteArrayOutputStream();
      PseudoDocument pd = bufferedDocuments.get(nameKey);
      pd.identifier = documentNumber;
      // This is a hack to make the document smaller
      if (pd.terms.size() > 1000000) {
	  pd.terms = pd.terms.subList(0, 1000000);
      }
      array.write(PseudoDocument.serialize(emptyParameters, pd));
      array.close();
      byte[] newKey = Utility.fromInt(pd.identifier);
      byte[] value = array.toByteArray();
      System.err.printf("Total stored document (%d) size: %d\n", pd.identifier, value.length);
      writer.add(new GenericElement(newKey, value));
      if (docsOut != null) {
        docsOut.increment();
      }
      ++documentNumber;
    }
    bufferedDocuments.clear();
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getJSON().isString("filename")) {
      handler.addError("DocumentAggregator requires a 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableFile(index, handler);
  }

  public void close() throws IOException {
    write();
    writer.close();
  }
}
