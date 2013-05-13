/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package julien.galago.core.parse.stem;

import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className="julien.galago.core.parse.Document")
@OutputClass(className="julien.galago.core.parse.Document")
public class NullStemmer extends Stemmer {

  @Override
  protected String stemTerm(String term) {
    return term;
  }
}
