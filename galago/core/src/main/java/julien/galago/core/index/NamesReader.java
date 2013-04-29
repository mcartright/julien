// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index;

import java.io.IOException;

/**
 *
 * @author irmarc
 */
public interface NamesReader extends IndexPartReader {

  public String getDocumentName(int document) throws IOException;

  public int getDocumentIdentifier(String document) throws IOException;

  public NamesIterator getNamesIterator() throws IOException;

  public interface NamesIterator extends Iterator {

    public String getCurrentName() throws IOException;

    public int getCurrentIdentifier() throws IOException;
  }
}
