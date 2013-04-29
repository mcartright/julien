// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.parse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import julien.galago.core.types.WordCount;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.Reducer;
import julien.galago.tupleflow.StandardStep;
import julien.galago.tupleflow.execution.Verified;


/**
 *
 * @author trevor
 */
@InputClass(className = "julien.galago.core.types.WordCount", order = {"+word"})
@OutputClass(className = "julien.galago.core.types.WordCount", order = {"+word"})
@Verified
public class WordCountReducer
        extends StandardStep<WordCount, WordCount>
        implements Reducer<WordCount>, WordCount.Processor {

  private WordCount last = null;
  private WordCount aggregate = null;
  private long totalTerms = 0;

  @Override
  public void process(WordCount wordCount) throws IOException {
    if (last != null) {
      if (!Arrays.equals(wordCount.word, last.word)) {
        flush();
      } else if (aggregate == null) {
        aggregate = new WordCount(last.word, last.count + wordCount.count,
                last.documents + wordCount.documents);
      } else {
        aggregate.count += wordCount.count;
        aggregate.documents += wordCount.documents;
      }
    }

    last = wordCount;
  }

  public void flush() throws IOException {
    if (last != null) {
      if (aggregate != null) {
        assert aggregate != null;
        processor.process(aggregate);
        totalTerms += aggregate.count;
      } else {
        assert last != null;
        processor.process(last);
        totalTerms += last.count;
      }

      aggregate = null;
    }
  }
  
  @Override
  public void close() throws IOException {
    flush();
    super.close();
  }

  // this won't work at the moment...
  @Override
  public ArrayList<WordCount> reduce(List<WordCount> input) throws IOException {
    HashMap<byte[], WordCount> countObjects = new HashMap<byte[], WordCount>();

    for (WordCount wordCount : input) {
      WordCount original = countObjects.get(wordCount.word);

      if (original == null) {
        countObjects.put(wordCount.word, wordCount);
      } else {
        original.documents += wordCount.documents;
        original.count += wordCount.count;
      }
    }

    return new ArrayList<WordCount>(countObjects.values());
  }

  public long getTotalTerms() {
    return totalTerms;
  }
}
