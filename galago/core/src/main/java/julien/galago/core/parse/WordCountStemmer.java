// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.parse;

import java.io.IOException;

import julien.galago.core.types.WordCount;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.StandardStep;
import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.execution.Verified;

import org.tartarus.snowball.ext.englishStemmer;

/**
 *
 * @author trevor
 */
@InputClass(className = "julien.galago.core.types.WordCount", order = {"+word"})
@OutputClass(className = "julien.galago.core.types.WordCount")
@Verified
public class WordCountStemmer extends StandardStep<WordCount, WordCount> {
  englishStemmer stemmer = new englishStemmer();
  
  public void process(WordCount wordCount) throws IOException {
    stemmer.setCurrent(Utility.toString(wordCount.word));
    byte[] stem;
    if(stemmer.stem()){
      stem = Utility.fromString(stemmer.getCurrent());
    } else {
      stem = wordCount.word;
    }
    processor.process(new WordCount(stem,wordCount.count,wordCount.documents));
  }
}
