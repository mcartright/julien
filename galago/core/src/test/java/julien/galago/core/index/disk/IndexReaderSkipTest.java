/*
 * BSD License (http://www.galagosearch.org/license)
 */
package julien.galago.core.index.disk;

import java.io.File;
import java.util.Random;

import julien.galago.core.index.Iterator;
import julien.galago.core.index.disk.CountIndexReader;
import julien.galago.core.index.disk.CountIndexWriter;
import julien.galago.core.index.disk.PositionIndexReader;
import julien.galago.core.index.disk.PositionIndexWriter;
import julien.galago.tupleflow.FakeParameters;
import julien.galago.tupleflow.Parameters;
import julien.galago.tupleflow.Utility;
import junit.framework.TestCase;

/**
 *
 * @author sjh
 */
public class IndexReaderSkipTest extends TestCase {

  public IndexReaderSkipTest(String name) {
    super(name);
  }

  public void testPositionIndexSkipping() throws Exception {
    Random r = new Random();
    File temp = Utility.createTemporary();

    try {
      Parameters parameters = new Parameters();
      parameters.set("filename", temp.getAbsolutePath());
      parameters.set("skipDistance", 10);
      parameters.set("skipResetDistance", 5);
      parameters.set("estimateDocumentCount", true);
      PositionIndexWriter writer = new PositionIndexWriter(new FakeParameters(parameters));

      writer.processWord(Utility.fromString("key"));
      for (int doc = 0; doc < 1000; doc++) {
        writer.processDocument(doc);
        for (int begin = 0; begin < 100; begin++) {
          writer.processPosition(begin);
        }
      }
      writer.close();

      PositionIndexReader reader = new PositionIndexReader(parameters.getString("filename"));
      for (int i = 1; i < 1000; i++) {
        Iterator extents = reader.getTermExtents(Utility.fromString("key"));
        extents.syncTo(i);
        assertEquals(extents.currentCandidate(), i);

      }

    } finally {
      temp.delete();
    }
  }

  public void testCountIndexSkipping() throws Exception {
    Random r = new Random();
    File temp = Utility.createTemporary();

    try {
      Parameters parameters = new Parameters();
      parameters.set("filename", temp.getAbsolutePath());
      parameters.set("skipDistance", 10);
      parameters.set("skipResetDistance", 5);
      CountIndexWriter writer = new CountIndexWriter(new FakeParameters(parameters));

      writer.processWord(Utility.fromString("key"));
      for (int doc = 0; doc < 1000; doc++) {
        writer.processDocument(doc);
        writer.processTuple(r.nextInt(128) + 128);
      }
      writer.close();

      CountIndexReader reader = new CountIndexReader(parameters.getString("filename"));
      for (int i = 1; i < 1000; i++) {
        Iterator counts = reader.getTermCounts(Utility.fromString("key"));
        counts.syncTo(i);
        assertEquals(counts.currentCandidate(), i);
      }

    } finally {
      temp.delete();
    }
  }
}
