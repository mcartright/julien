// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.parse.stem;

import org.tartarus.snowball.ext.englishStemmer;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.execution.Verified;

/**
 *
 * @author trevor
 * sjh: modified to accept numbered documents as required.
 */
@Verified
@InputClass(className="julien.galago.core.parse.Document")
@OutputClass(className="julien.galago.core.parse.Document")
public class Porter2Stemmer extends Stemmer {

  englishStemmer stemmer = new englishStemmer();

  @Override
  protected String stemTerm(String term) {
    String stem = term;
    stemmer.setCurrent(term);
    if (stemmer.stem()) {
      stem = stemmer.getCurrent();
    }
    return stem;
  }
}
