/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package julien.galago.core.parse;

import java.io.IOException;

import julien.galago.core.types.KeyValuePair;
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
@InputClass(className="java.lang.String")
@OutputClass(className="julien.galago.core.types.KeyValuePair")
public class LineSplitter extends StandardStep<String, KeyValuePair> {
  
  String split;
  
  public LineSplitter(TupleFlowParameters p) {
    split = p.getJSON().get("split", "\t");
  }
  
  @Override
  public void process(String line) throws IOException {
    if (line.length() == 0) {
      return;
    }
    
    String[] parts = line.split(split, 2);
    
    if (parts.length == 2) {
      processor.process(
              new KeyValuePair(
              Utility.fromString(parts[0]),
              Utility.fromString(parts[1])));
    } else {
      processor.process(
              new KeyValuePair(
              Utility.fromString(parts[0]),
              new byte[0]));
    }
  }
}
