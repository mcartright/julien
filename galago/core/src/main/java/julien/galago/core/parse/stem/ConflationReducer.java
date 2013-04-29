/*
 *  BSD License (http://www.galagosearch.org/license)
 */
package julien.galago.core.parse.stem;

import java.io.IOException;

import julien.galago.core.parse.Document;
import julien.galago.core.types.KeyValuePair;
import julien.galago.tupleflow.*;
import julien.galago.tupleflow.execution.Verified;


/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "julien.galago.core.types.KeyValuePair", order = {"+key", "+value"})
@OutputClass(className = "julien.galago.core.types.KeyValuePair", order = {"+key", "+value"})
public class ConflationReducer extends StandardStep<KeyValuePair, KeyValuePair> implements KeyValuePair.Source {

  private KeyValuePair previous = null;

  @Override
  public void process(KeyValuePair kvp) throws IOException {
    if (previous == null){
      processor.process(kvp);
    } else if( Utility.compare(previous.key, kvp.key) == 0 
            && Utility.compare(previous.value, kvp.value) == 0){
      // identical conflations - already processed previous - so do nothing
    } else {
      // otherwise different conflations - process kvp
      processor.process(kvp);
    }
    // update previous
    previous = kvp;
  }
}
