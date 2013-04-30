// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index;

import java.io.File;

import julien.galago.core.index.disk.DiskNameReader;
import julien.galago.core.index.disk.DiskNameReverseReader;
import julien.galago.core.index.disk.DiskNameReverseWriter;
import julien.galago.core.index.disk.DiskNameWriter;
import julien.galago.core.types.NumberedDocumentData;
import julien.galago.tupleflow.FakeParameters;
import julien.galago.tupleflow.Parameters;
import julien.galago.tupleflow.Utility;
import junit.framework.TestCase;


/**
 *
 * @author sjh
 */
public class DocumentNameTest extends TestCase {

  public DocumentNameTest(String testName) {
    super(testName);
  }

  public void testDocumentNameStore() throws Exception {
    File f = null;
    try {
      Parameters p = new Parameters();
      f = Utility.createTemporaryDirectory();
      File names = new File(f.getAbsolutePath() + File.separator + "names");
      p.set("filename", names.getAbsolutePath());
      FakeParameters params = new FakeParameters(p);
      DiskNameWriter writer = new DiskNameWriter(params);

      for (int key_val = 10; key_val < 35; key_val++) {
        String str_val = "document_name_key_is_" + key_val;
        NumberedDocumentData ndd = new NumberedDocumentData(str_val, "", "", key_val, 0);
        writer.process(ndd);
      }
      writer.close();

      DiskNameReader reader = new DiskNameReader(names.getAbsolutePath());

      int key = 15;
      String name = "document_name_key_is_" + key;

      String result1 = reader.getDocumentName(key);
      assert name.equals(result1);

    } finally {
      if (f != null) {
        Utility.deleteDirectory(f);
      }
    }
  }

  public void testDocumentNameReverseStore() throws Exception {
    File f = null;
    try {
      Parameters p = new Parameters();
      f = Utility.createTemporaryDirectory();
      File names = new File(f.getAbsolutePath() + File.separator + "names.reverse");
      p.set("filename", names.getAbsolutePath());
      FakeParameters params = new FakeParameters(p);
      DiskNameReverseWriter writer = new DiskNameReverseWriter(params);

      for (int key_val = 10; key_val < 35; key_val++) {
        String str_val = "document_name_key_is_" + key_val;
        NumberedDocumentData ndd = new NumberedDocumentData(str_val, "", "", key_val, 0);
        writer.process(ndd);
      }
      writer.close();

      DiskNameReverseReader reader = new DiskNameReverseReader(names.getAbsolutePath());

      int key = 15;
      String name = "document_name_key_is_" + key;

      int result2 = reader.getDocumentIdentifier(name);
      assert key == result2;
    } finally {
      if (f != null) {
        Utility.deleteDirectory(f);
      }
    }
  }
}
