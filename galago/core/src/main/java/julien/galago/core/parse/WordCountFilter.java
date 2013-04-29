// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.parse;

import java.io.IOException;

import julien.galago.core.types.WordCount;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.StandardStep;
import julien.galago.tupleflow.TupleFlowParameters;
import julien.galago.tupleflow.execution.Verified;


/**
 *
 * @author trevor
 */
@InputClass(className = "julien.galago.core.types.WordCount", order = {"+word"})
@OutputClass(className = "julien.galago.core.types.WordCount", order = {"+word"})
@Verified
public class WordCountFilter extends StandardStep<WordCount, WordCount> {

  private long minThreshold = 2;

  public WordCountFilter(TupleFlowParameters p) {
    minThreshold = p.getJSON().get("minThreshold", minThreshold);
  }

  public void process(WordCount wordCount) throws IOException {
    if(wordCount.count >= minThreshold){
      processor.process(wordCount);
    }
  }
}
