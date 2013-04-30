/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package julien.galago.core.parse;

import java.io.IOException;

import julien.galago.core.types.DocumentIndicator;
import julien.galago.core.types.NumberKeyValue;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.StandardStep;
import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.execution.Verified;


/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "julien.galago.core.types.NumberKeyValue")
@OutputClass(className = "julien.galago.core.types.DocumentIndicator")
public class IndicatorExtractor extends StandardStep<NumberKeyValue, DocumentIndicator> {

  @Override
  public void process(NumberKeyValue nkvp) throws IOException {
    if (nkvp.value.length == 0) {
      processor.process(new DocumentIndicator(nkvp.number, true));
    } else {
      processor.process(new DocumentIndicator(nkvp.number,
              Boolean.parseBoolean(Utility.toString(nkvp.value))));
    }
  }
}
