// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index;

import java.io.IOException;

/**
 *
 * @author irmarc
 */
public interface LengthsReader extends Index.IndexPartReader {

  public int getLength(int document) throws IOException;

  public LengthsIterator getLengthsIterator() throws IOException;

  public interface LengthsIterator extends Iterator {
    // This function returns the name of the region:
    // e.g. document, field-name, or #inside(field-name field-name)
    public byte[] getRegionBytes();

    public int getCurrentLength();

    public int getCurrentIdentifier();
  }
}
