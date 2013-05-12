/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package julien.galago.core.parse.stem;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import julien.galago.core.parse.Document;
import julien.galago.tupleflow.StandardStep;
import julien.galago.tupleflow.TupleFlowParameters;
import julien.galago.tupleflow.execution.ErrorStore;

/**
 *
 *
 * @author sjh
 */
public abstract class Stemmer extends StandardStep<Document, Document> {

  // each instance of Stemmer should have it's own lock
  final Object lock = new Object();

  long cacheLimit = 50000;
  HashMap<String, String> cache = new HashMap();

  @Override
  public void process(Document document) throws IOException {
    processor.process(stem(document));
  }

  public static String[] getOutputOrder(TupleFlowParameters parameters) {
    return new String[0];
  }

  public Document stem(Document document) {
    // new document is necessary - stemmed terms were being propagated unintentially
    Document newDocument = new Document(document);
    List<String> words = newDocument.terms;
    for (int i = 0; i < words.size(); i++) {
      String word = words.get(i);
      words.set(i, stem(word));
    }
    return newDocument;
  }

  public String stem(String term) {
    if (cache.containsKey(term)) {
      return cache.get(term);
    }
    String stemmedTerm;

    synchronized (lock) {
      stemmedTerm = stemTerm(term);
    }

    if (!cache.containsKey(stemmedTerm)) {
      cache.put(term, stemmedTerm);
    }
    if (cache.size() > cacheLimit) {
      cache.clear();
    }
    return stemmedTerm;
  }

  // This function should only be use synchronously (see 'lock')
  protected abstract String stemTerm(String term);

  /**
   * allows stemming of windows.
   */
  public String stemWindow(String term) {
    StringBuilder window = new StringBuilder();
    for (String t : term.split("~")) {
      if (window.length() > 0) {
        window.append("~");
      }
      window.append(stem(t));
    }
    return window.toString();
  }

  public Class<Document> getInputClass() {
      return Document.class;
  }

  public Class<Document> getOutputClass() {
      return Document.class;
  }

  public static void verify(
			    TupleFlowParameters parameters,
			    ErrorStore handler)
      throws IOException {
      // nothing needed - just doing this for inheritance
  }
}
