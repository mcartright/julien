/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package julien.galago.core.parse;

import java.io.File;
import java.io.IOException;

import julien.galago.core.index.disk.DiskIndex;
import julien.galago.core.index.disk.DiskNameReader;
import julien.galago.core.index.disk.DiskNameReverseReader;
import julien.galago.core.types.KeyValuePair;
import julien.galago.core.types.NumberKeyValue;
import julien.galago.tupleflow.Counter;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.StandardStep;
import julien.galago.tupleflow.TupleFlowParameters;
import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.execution.Verified;


/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "julien.galago.core.types.KeyValuePair")
@OutputClass(className = "julien.galago.core.types.NumberKeyValue")
public class NumberKeyValuePairs extends StandardStep<KeyValuePair, NumberKeyValue> {

  private final DiskNameReverseReader.KeyIterator namesIterator;
  private Counter numbered;

  public NumberKeyValuePairs(TupleFlowParameters parameters) throws IOException {
    String namesPath = parameters.getJSON().getString("indexPath") + File.separator + "names.reverse";
    namesIterator = ((DiskNameReverseReader) DiskIndex.openIndexReader(namesPath)).keys();
    numbered = parameters.getCounter("Numbered Items");
  }

  @Override
  public void process(KeyValuePair kvp) throws IOException {
    if (!namesIterator.isDone()) {
      if (namesIterator.skipToKey(kvp.key)) {
        if (Utility.compare(namesIterator.getKey(), kvp.key) == 0) {
          if (numbered != null) {
            numbered.increment();
          }
          processor.process(new NumberKeyValue(namesIterator.getCurrentIdentifier(), kvp.key, kvp.value));
        }
      }
    }
  }
}
