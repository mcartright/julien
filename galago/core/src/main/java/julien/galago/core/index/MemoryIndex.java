/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package julien.galago.core.index;

import java.io.IOException;
import julien.galago.core.index.disk.DiskBTreeReader;
import julien.galago.core.index.disk.DiskIndex;

/**
 *
 * @author irmarc
 */
public class MemoryIndex extends DiskIndex {

  public MemoryIndex(String indexPath) throws IOException {
    this(indexPath, true);
  }

  /**
   * The second parameter turns off eager loading of index parts. Useful if you
   * want caching of the parts used heavily, but don't plan to delete the
   * underlying index.
   */
  public MemoryIndex(String indexPath, boolean eagerLoad) throws IOException {
    super(indexPath);

    if (eagerLoad) {
      for (String part : partNames) {
        IndexPartReader ignored = this.<IndexPartReader>getPart(part);
      }
    }
  }

  @Override
  public BTreeReader getBTree(String path)
          throws IOException {
    BTreeReader reader = super.getBTree(path);
    BTreeReader oldReader = reader;
    assert (reader != null);
    reader = new MemoryBTree(reader);
    oldReader.close();
    return reader;
  }
}
