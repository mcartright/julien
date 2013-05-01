// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index.dynamic;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.logging.Logger;

import julien.galago.core.parse.Document;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.StandardStep;
import julien.galago.tupleflow.TupleFlowParameters;
import julien.galago.tupleflow.execution.Verified;



@Verified
@InputClass( className = "julien.galago.core.parse.Document")
@OutputClass( className = "julien.galago.core.parse.Document")
public class MemoryChecker extends StandardStep<Document, Document> {
  
  private Logger logger = Logger.getLogger(MemoryChecker.class.toString());
  MemoryPoolMXBean heap = null;
  long docCount = 0;
  long checkFreq = 1000;
  
  public MemoryChecker(TupleFlowParameters params){
    heap = getMemoryBean();
    checkFreq = params.getJSON().get("checkFreq", 1000);
  }
  
  @Override
  public void process(Document doc) throws IOException {
    if(docCount % checkFreq == 0){
      //System.err.print("count:" + docCount + " ");
      printMemoryUsage(heap);
    }
    docCount ++;
    
    processor.process(doc);
  }
  
  private static void printMemoryUsage(MemoryPoolMXBean heap){
    System.gc();

    MemoryUsage usage = heap.getUsage();
    System.err.println(heap.getName() + " -usage- " + usage.getUsed() + " -human- " + (usage.getUsed() / 1024.0 / 1024.0));
  }
  
  private static MemoryPoolMXBean getMemoryBean(){
    System.gc();

    List<MemoryPoolMXBean> pools = ManagementFactory.getMemoryPoolMXBeans();
    long curMax = 0;
    MemoryPoolMXBean heap = null;
    
    for (MemoryPoolMXBean pool : pools) {
      if (pool.getType() != MemoryType.HEAP) {
        continue;
      }
      MemoryUsage memusage = pool.getUsage();
      long max = memusage.getMax();
      if(max > curMax)
        heap = pool;
    }
    return heap;
  }
}
