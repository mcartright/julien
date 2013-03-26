// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.lemurproject.galago.core.index.AggregateReader.IndexPartStatistics;
import org.lemurproject.galago.core.index.LengthsReader.LengthsIterator;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * Describes the general contract that must be fulfilled by an implementing class.
 * @author irmarc
 */
public interface Index {

  public interface IndexComponentReader {
    public Parameters getManifest();
    public void close() throws IOException;
  }

  public IndexPartReader getIndexPart(String part) throws IOException;

  public boolean containsIdentifier(int identifer) throws IOException;

  public boolean containsPart(String partName);

  public Iterator getIterator(byte[] key, Parameters p) throws IOException;

  public IndexPartStatistics getIndexPartStatistics(String part);

  public void close() throws IOException;

  public int getLength(int identifier) throws IOException;

  public String getName(int identifier) throws IOException;

  public int getIdentifier(String name) throws IOException;

  public Document getItem(String name, Parameters p) throws IOException;

  public Map<String,Document> getItems(List<String> names, Parameters p) throws IOException;

  public LengthsIterator getLengthsIterator() throws IOException;

  public NamesReader.NamesIterator getNamesIterator() throws IOException;

  public Parameters getManifest();

  public Set<String> getPartNames();
}
