/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package julien.galago.core.parse;

import java.io.IOException;

import julien.galago.core.types.DocumentFeature;
import julien.galago.core.types.NumberKeyValue;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.StandardStep;
import julien.galago.tupleflow.TupleFlowParameters;
import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.execution.Verified;


/**
 * Vanilla implementation
 * 
 * 
 * @author sjh
 */
@Verified
@InputClass(className = "julien.galago.core.types.NumberKeyValue")
@OutputClass(className = "julien.galago.core.types.DocumentFeature")
public class PriorExtractor extends StandardStep<NumberKeyValue, DocumentFeature> {

  private boolean applylog;

  public PriorExtractor(TupleFlowParameters parameters) throws IOException {
    // type of scores being read in:
    String priorType = parameters.getJSON().get("priorType", "raw");
    applylog = false;
    if (priorType.startsWith("prob")) { //prob
      applylog = true;
    }
  }

  @Override
  public void process(NumberKeyValue nkvp) throws IOException {
    if (nkvp.value.length > 0) {
      double val = Double.parseDouble(Utility.toString(nkvp.value));
      if (applylog) {
        val = Math.log(val);
      }
      processor.process(new DocumentFeature(nkvp.number, val));
    }
  }
}
