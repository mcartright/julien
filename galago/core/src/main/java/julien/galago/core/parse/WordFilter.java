// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.parse;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.NullProcessor;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.StandardStep;
import julien.galago.tupleflow.TupleFlowParameters;
import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.execution.ErrorHandler;

/**
 * WordFilter filters out unnecessary words from documents.  Typically this object
 * takes a stopword list as parameters and removes all the listed words.  However, 
 * this can also be used to keep only the specified list of words in the index, which
 * can be used to create an index that is tailored for only a small set
 * of experimental queries.
 * 
 * @author trevor
 */
@InputClass(className = "julien.galago.core.parse.Document")
@OutputClass(className = "julien.galago.core.parse.Document")
public class WordFilter extends StandardStep<Document, Document> {

  Set<String> stopwords = new HashSet<String>();
  boolean keepListWords = false;

  public WordFilter(HashSet<String> words) {
    stopwords = words;
    processor = new NullProcessor(Document.class);
  }

  public WordFilter(TupleFlowParameters params) throws IOException {
    if (params.getJSON().containsKey("filename")) {
      String filename = params.getJSON().getString("filename");
      stopwords = Utility.readFileToStringSet(new File(filename));
    } else {
      stopwords = new HashSet(params.getJSON().getList("stopwords"));
    }

    keepListWords = params.getJSON().get("keepListWords", false);
  }

  @Override
  public void process(Document document) throws IOException {
    List<String> words = document.terms;

    for (int i = 0; i < words.size(); i++) {
      String word = words.get(i);
      boolean wordInList = stopwords.contains(word);
      boolean removeWord = wordInList != keepListWords;

      if (removeWord) {
        words.set(i, null);
      }
    }

    processor.process(document);
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (parameters.getJSON().containsKey("filename")) {
      return;
    }
    if (parameters.getJSON().getList("word").isEmpty()) {
      handler.addWarning("Couldn't find any words in the stopword list.");
    }
  }
}
