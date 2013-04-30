/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package julien.galago.core.parse;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

import julien.galago.core.types.DocumentSplit;
import julien.galago.tupleflow.Counter;
import julien.galago.tupleflow.ExNihiloSource;
import julien.galago.tupleflow.IncompatibleProcessorException;
import julien.galago.tupleflow.Linkage;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.Parameters;
import julien.galago.tupleflow.Processor;
import julien.galago.tupleflow.Step;
import julien.galago.tupleflow.TupleFlowParameters;
import julien.galago.tupleflow.execution.Verified;


/**
 *
 * @author sjh
 */
@Verified
@OutputClass(className = "java.lang.String")
public class FileLineParser implements ExNihiloSource<String> {

  public Processor<String> processor;
  Parameters p;
  Counter lines;

  public FileLineParser(TupleFlowParameters parameters) {
    p = parameters.getJSON();
    lines = parameters.getCounter("File Lines Read");
  }

  @Override
  public void run() throws IOException {
    BufferedReader reader;
    for (String f : (List<String>) p.getList("inputPath")) {
      DocumentSplit split = new DocumentSplit();
      split.fileName = f;
      split.isCompressed = ( f.endsWith(".gz") || f.endsWith(".bz") );
      reader = DocumentStreamParser.getBufferedReader( split );
      String line;
      while (null != (line = reader.readLine())) {
        if(lines != null) lines.increment();

        if (line.startsWith("#")) {
          continue;
        }
        processor.process(line);
      }
      reader.close();
    }
    processor.close();
  }

  @Override
  public Step setProcessor(Step processor) throws IncompatibleProcessorException {
    Linkage.link(this, processor);
    return processor;
  }
}
